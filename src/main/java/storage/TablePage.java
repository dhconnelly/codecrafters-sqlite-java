package storage;

import java.nio.ByteBuffer;

public sealed abstract class TablePage<T>
    extends Page<T> permits TablePage.Leaf, TablePage.Interior {
  protected TablePage(Database db, ByteBuffer buf, int base) {
    super(db, buf, base);
  }

  static final class Leaf extends TablePage<Leaf.Row> {
    public Leaf(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
    }

    private static Cell parseCell(ByteBuffer buf, int cellOffset) {
      var payloadSize = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += payloadSize.size();
      var rowId = VarInt.parseFrom(buf.position(cellOffset));
      cellOffset += rowId.size();
      var payload = new byte[(int) payloadSize.value()];
      buf.position(cellOffset).get(payload);
      // TODO: overflow pages
      return new Cell(rowId.value(), payload);
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
      var cell = parseCell(buf, cellOffset(index));
      return new Row(cell.rowId(), Record.parse(db, cell.payload()));
    }

    private record Cell(long rowId, byte[] payload) {}
    public record Row(long rowId, Record values) {}
  }

  static final class Interior extends TablePage<IndexedPage<Long>> {
    private final int rightPage;

    public Interior(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
      rightPage = buf.position(base + 8).getInt();
    }

    @Override
    protected int headerSize() {return 12;}

    @Override
    protected int numRecords() {return numCells + 1;}

    @Override
    protected IndexedPage<Long> parseRecord(int index, ByteBuffer buf) {
      if (index == 0) {
        var cell = parseCell(index, buf);
        return new IndexedPage<>(new IndexedPage.Unbounded<>(),
                                 new IndexedPage.Bounded<>(cell.payload),
                                 cell.pageNumber());
      } else if (index == numCells) {
        var cell = parseCell(index - 1, buf);
        return new IndexedPage<>(new IndexedPage.Bounded<>(cell.payload),
                                 new IndexedPage.Unbounded<>(),
                                 rightPage);
      } else {
        var prev = parseCell(index - 1, buf);
        var cur = parseCell(index, buf);
        return new IndexedPage<>(new IndexedPage.Bounded<>(prev.payload),
                                 new IndexedPage.Bounded<>(cur.payload),
                                 cur.pageNumber());
      }
    }

    private Cell parseCell(int index, ByteBuffer buf) {
      if (index >= numCells) throw new AssertionError("index < numCells");
      int offset = cellOffset(index);
      int pageNumber = buf.position(offset).getInt();
      var rowId = VarInt.parseFrom(buf.position(offset + 4));
      return new Cell(pageNumber, rowId.value());
    }

    private record Cell(int pageNumber, long payload) {}
  }
}
