package storage;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Page {
  private final Database db;
  private final ByteBuffer buf;
  private final int base;
  private final int headerSize;
  private final short numCells;
  private final Type type;

  public Page(Database db, ByteBuffer buf, int base)
  throws DatabaseException {
    this.db = db;
    this.base = base;
    this.buf = buf;
    this.numCells = buf.position(base + 3).getShort();
    this.type = typeOf(buf.position(base).get());
    this.headerSize = switch (type) {
      case InteriorIndex, InteriorTable -> 12;
      case LeafIndex, LeafTable -> 8;
    };
  }

  private static Type typeOf(byte first) throws DatabaseException {
    return switch (first) {
      case 0x02 -> Type.InteriorIndex;
      case 0x05 -> Type.InteriorTable;
      case 0x0a -> Type.LeafIndex;
      case 0x0d -> Type.LeafTable;
      default ->
          throw new DatabaseException("invalid page type: %x".formatted(first));
    };
  }

  private static List<Value> parseRecord(Database db, byte[] payload)
  throws DatabaseException {
    var values = new ArrayList<Value>();
    ByteBuffer buf = ByteBuffer.wrap(payload);
    var headerSize = VarInt.parseFrom(buf.position(0));
    int headerOffset = headerSize.size();
    int contentOffset = headerSize.value();
    while (headerOffset < headerSize.value()) {
      var serialType = VarInt.parseFrom(buf.position(headerOffset));
      headerOffset += serialType.size();
      int n = serialType.value();
      // TODO: introduce a SizedValue and simplify this
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
            throw new DatabaseException("invalid serial type: %d".formatted(n));
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
              throw new DatabaseException("invalid charset: " + charset, e);
            }
            yield (n - 13) / 2;
          }
        }
      };
    }
    return values;
  }

  // TODO: stream?
  private List<TableLeafCell> readCells() {
    int pointerOffset = base + headerSize;
    var cells = new ArrayList<TableLeafCell>();
    for (int i = 0; i < numCells; i++) {
      int cellOffset = buf.position(pointerOffset + i * 2).getShort();
      cells.add(switch (type) {
        case Type.LeafTable -> TableLeafCell.parse(buf, cellOffset);
        default -> throw new IllegalArgumentException("TODO");
      });
    }
    return cells;
  }

  public List<List<Value>> records() throws DatabaseException {
    // TODO: stream with exceptions
    var records = new ArrayList<List<Value>>();
    for (var cell : readCells()) {
      records.add(parseRecord(db, cell.payload()));
    }
    return records;
  }

  private enum Type {InteriorIndex, InteriorTable, LeafIndex, LeafTable}

  private record TableLeafCell(int rowId, byte[] payload) {
    static TableLeafCell parse(ByteBuffer buf, int cellOffset) {
      var payloadSize = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += payloadSize.size();
      var rowId = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += rowId.size();
      var payload = new byte[payloadSize.value()];
      buf.position(cellOffset).get(payload);
      return new TableLeafCell(rowId.value(), payload);
    }
  }
}
