package storage;

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

  public List<Record> readRecords() throws Record.FormatException {
    // TODO: stream with exceptions
    var records = new ArrayList<Record>();
    for (var cell : readCells()) {
      records.add(Record.parse(db, cell.payload()));
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
