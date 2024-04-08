package storage;

import java.nio.ByteBuffer;

public sealed abstract class IndexPage<T>
    extends Page<T> permits IndexPage.Interior, IndexPage.Leaf {
  protected IndexPage(Database db, ByteBuffer buf, int base) {
    super(db, buf, base);
  }

  static final class Interior extends IndexPage {
    Interior(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
    }

    @Override
    protected int headerSize() {
      return 0;
    }

    @Override
    protected int numRecords() {
      return 0;
    }

    @Override
    protected Object parseRecord(int index, ByteBuffer buf)
    throws DatabaseException {
      return null;
    }
  }

  static final class Leaf extends IndexPage {
    Leaf(Database db, ByteBuffer buf, int base) {
      super(db, buf, base);
    }

    @Override
    protected int headerSize() {
      return 0;
    }

    @Override
    protected int numRecords() {
      return 0;
    }

    @Override
    protected Object parseRecord(int index, ByteBuffer buf)
    throws DatabaseException {
      return null;
    }
  }
}
