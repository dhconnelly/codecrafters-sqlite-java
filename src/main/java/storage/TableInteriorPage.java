package storage;

import java.nio.ByteBuffer;

public final class TableInteriorPage extends Page<IndexedPage> {
  private final int rightPage;

  public TableInteriorPage(Database db, ByteBuffer buf, int base) {
    super(db, buf, base);
    rightPage = buf.position(base + 8).getInt();
  }

  @Override
  protected int headerSize() {return 12;}

  @Override
  protected int numRecords() {return numCells + 1;}

  @Override
  protected IndexedPage parseRecord(int index, ByteBuffer buf) {
    if (index == 0) {
      var cell = parseCell(index, buf);
      return new IndexedPage(new IndexedPage.Unbounded(),
                             new IndexedPage.Bounded(cell.rowId),
                             cell.pageNumber());
    } else if (index == numCells) {
      var cell = parseCell(index - 1, buf);
      return new IndexedPage(new IndexedPage.Bounded(cell.rowId),
                             new IndexedPage.Unbounded(),
                             rightPage);
    } else {
      var prev = parseCell(index - 1, buf);
      var cur = parseCell(index, buf);
      return new IndexedPage(new IndexedPage.Bounded(prev.rowId),
                             new IndexedPage.Bounded(cur.rowId),
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

  private record Cell(int pageNumber, int rowId) {}
}
