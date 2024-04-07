package storage;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public final class TableLeafPage extends Page<TableLeafPage.Row> {
  public TableLeafPage(Database db, ByteBuffer buf, int base) {
    super(db, buf, base);
  }

  @Override
  protected int headerSize() {
    return 8;
  }

  @Override
  protected int numRecords() {return numCells;}

  @Override
  protected Row parseRecord(int index, ByteBuffer buf)
  throws DatabaseException {
    return parseRecord(db, parseCell(buf, cellOffset(index)));
  }

  private static Cell parseCell(ByteBuffer buf, int cellOffset) {
    var payloadSize = VarInt.parseFrom(buf.position(cellOffset));
    cellOffset += payloadSize.size();
    var rowId = VarInt.parseFrom(buf.position(cellOffset));
    cellOffset += rowId.size();
    var payload = new byte[payloadSize.value()];
    buf.position(cellOffset).get(payload);
    cellOffset += payloadSize.value();
    // TODO: this should be based on whether it runs into the next offset, not
    // comparing against the overall buffer limit
    OptionalInt overflowPage = cellOffset == buf.limit()
        ? OptionalInt.empty()
        : OptionalInt.of(buf.position(cellOffset).getInt());
    return new Cell(rowId.value(), payload, overflowPage);
  }

  private static Row parseRecord(Database db, Cell cell)
  throws DatabaseException {
    var values = new ArrayList<Value>();
    ByteBuffer buf = ByteBuffer.wrap(cell.payload);
    var headerSize = VarInt.parseFrom(buf.position(0));
    int headerOffset = headerSize.size();
    int contentOffset = headerSize.value();
    while (headerOffset < headerSize.value()) {
      var serialType = VarInt.parseFrom(buf.position(headerOffset));
      headerOffset += serialType.size();
      int n = serialType.value();
      var sizedValue = switch (n) {
        case 0 -> new SizedValue(0, new Value.NullValue());
        case 1 -> new SizedValue(1, new Value.IntValue(
            buf.position(contentOffset).get()));
        case 2 -> new SizedValue(2, new Value.IntValue(
            buf.position(contentOffset).getShort()));
        case 4 -> new SizedValue(4, new Value.IntValue(
            buf.position(contentOffset).getInt()));
        case 8 -> new SizedValue(0, new Value.IntValue(0));
        case 9 -> new SizedValue(0, new Value.IntValue(1));
        default -> {
          if (n < 12) {
            throw new DatabaseException("invalid serial type: %d".formatted(n));
          } else if (n % 2 == 0) {
            var blob = new byte[(n - 12) / 2];
            buf.position(contentOffset).get(blob);
            yield new SizedValue((n - 12) / 2, new Value.BlobValue(blob));
          } else {
            var data = new byte[(n - 13) / 2];
            buf.position(contentOffset).get(data);
            String charset = db.charset();
            try {
              yield new SizedValue((n - 13) / 2, new Value.StringValue(
                  new String(data, charset)));
            } catch (UnsupportedEncodingException e) {
              throw new DatabaseException("invalid charset: " + charset, e);
            }
          }
        }
      };
      values.add(sizedValue.value());
      contentOffset += sizedValue.size();
    }
    return new Row(cell.rowId(), values);
  }

  private record SizedValue(int size, Value value) {}
  private record Cell(int rowId, byte[] payload, OptionalInt overflowPage) {}
  public record Row(int rowId, List<Value> values) {}
}
