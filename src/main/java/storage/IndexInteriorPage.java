package storage;

import java.nio.ByteBuffer;

public final class IndexInteriorPage extends Page {
  protected IndexInteriorPage(Database db, ByteBuffer buf, int base) {
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
