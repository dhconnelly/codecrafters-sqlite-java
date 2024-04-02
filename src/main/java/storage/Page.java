package storage;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Page {
  private final Database db;
  private final ByteBuffer buf;
  private final int base;
  private final int headerSize;
  private final short numCells;

  public Page(Database db, ByteBuffer buf, int base) throws FormatException {
    this.db = db;
    this.base = base;
    this.buf = buf;
    this.numCells = buf.position(base + 3).getShort();
    this.headerSize = switch (typeOf(buf.position(base).get())) {
      case InteriorIndex, InteriorTable -> 12;
      case LeafIndex, LeafTable -> 8;
    };
  }

  private static Type typeOf(byte first) throws FormatException {
    return switch (first) {
      case 0x02 -> Type.InteriorIndex;
      case 0x05 -> Type.InteriorTable;
      case 0x0a -> Type.LeafIndex;
      case 0x0d -> Type.LeafTable;
      default ->
          throw new FormatException("invalid page type: %x".formatted(first));
    };
  }

  // TODO: stream?
  private List<TableLeafCell> readCells() {
    int pointerOffset = base + headerSize;
    var cells = new ArrayList<TableLeafCell>();
    for (int i = 0; i < numCells; i++) {
      int cellOffset = buf.position(pointerOffset + i * 2).getShort();
      var payloadSize = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += payloadSize.size();
      var rowId = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += rowId.size();
      var payload = new byte[payloadSize.value()];
      buf.position(cellOffset).get(payload);
      cellOffset += payloadSize.value();
      Optional<Integer> overflowPage =
          cellOffset == buf.limit() ? Optional.empty() : Optional.of(
              buf.position(cellOffset).getInt());
      cells.add(new TableLeafCell(rowId.value(), payload, overflowPage));
    }
    return cells;
  }

  private static List<Value> parseRecord(Database db, byte[] payload) throws Record.FormatException {
    var values = new ArrayList<Value>();
    ByteBuffer buf = ByteBuffer.wrap(payload);
    var headerSize = VarInt.parseFrom(buf.position(0));
    int headerOffset = headerSize.size();
    int contentOffset = headerSize.value();
    while (headerOffset < headerSize.value()) {
      var serialType = VarInt.parseFrom(buf.position(headerOffset));
      headerOffset += serialType.size();
      int n = serialType.value();
      contentOffset += switch (n) {
        case 0 -> {
          values.add(new Value.NullValue());
          yield 0;
        }
        case 1 -> {
          values.add(new Value.IntValue(buf.position(contentOffset).get()));
          yield 1;
        }
        case 2 -> {
          values.add(
              new Value.IntValue(buf.position(contentOffset).getShort()));
          yield 2;
        }
        case 4 -> {
          values.add(new Value.IntValue(buf.position(contentOffset).getInt()));
          yield 4;
        }
        default -> {
          if (n < 12) {
            throw new Record.FormatException(
                "invalid serial type: %d".formatted(n));
          }
          if (n % 2 == 0) {
            var blob = new byte[(n - 12) / 2];
            buf.position(contentOffset).get(blob);
            values.add(new Value.BlobValue(blob));
            yield (n - 12) / 2;
          } else {
            var data = new byte[(n - 13) / 2];
            buf.position(contentOffset).get(data);
            String charset = db.charset();
            try {
              values.add(new Value.StringValue(new String(data, charset)));
            } catch (UnsupportedEncodingException e) {
              throw new AssertionError("invalid charset: " + charset);
            }
            yield (n - 13) / 2;
          }
        }
      };
    }
    return values;
  }

  public List<List<Value>> records() throws Record.FormatException {
    // TODO: stream with exceptions
    var records = new ArrayList<List<Value>>();
    for (var cell : readCells()) {
      records.add(parseRecord(db, cell.payload()));
    }
    return records;
  }

  private enum Type {InteriorIndex, InteriorTable, LeafIndex, LeafTable}

  public static final class FormatException extends Exception {
    public FormatException(String message) {
      super(message);
    }
  }

  private record TableLeafCell(int rowId, byte[] payload,
                               Optional<Integer> overflowPage) {}
}