package storage;

import java.nio.ByteBuffer;

public sealed abstract class IndexPage<T>
    extends Page<T> permits IndexPage.Interior, IndexPage.Leaf {
  protected IndexPage(Database db, ByteBuffer buf, int base) {
    super(db, buf, base);
  }

  static final class Interior extends IndexPage<IndexedPage<byte[]>> {
    private final int rightPage;

    Interior(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
      rightPage = buf.position(base + 8).getInt();
    }

    @Override
    protected int headerSize() {return 12;}

    @Override
    protected int numRecords() {return numCells + 1;}

    // TODO: unify this with the table pages
    @Override
    protected IndexedPage<byte[]> parseRecord(int index, ByteBuffer buf) {
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
      offset += 4;
      var payloadSize = VarInt.parseFrom(buf.position(offset));
      offset += payloadSize.size();
      var payload = new byte[(int) payloadSize.value()];
      buf.position(offset).get(payload);
      return new Cell(pageNumber, payload);
    }

    private record Cell(int pageNumber, byte[] payload) {}
  }

  static final class Leaf extends IndexPage<byte[]> {
    Leaf(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
    }

    @Override
    protected int headerSize() {return 8;}

    @Override
    protected int numRecords() {return numCells;}

    @Override
    protected byte[] parseRecord(int index, ByteBuffer buf) {
      if (index >= numCells) throw new AssertionError("index < numCells");
      int offset = cellOffset(index);
      var payloadSize = VarInt.parseFrom(buf.position(offset));
      offset += payloadSize.size();
      var payload = new byte[(int) payloadSize.value()];
      buf.position(offset).get(payload);
      return payload;
    }
  }
}
