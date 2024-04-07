package storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public sealed abstract class Page<RecordType>
    permits TableInteriorPage, TableLeafPage {
  protected final Database db;
  private final ByteBuffer buf;
  private final int base;
  protected final short numCells;

  public static Page<?> create(Database db, ByteBuffer buf, int base)
  throws DatabaseException {
    byte first = buf.position(base).get();
    return switch (first) {
      case 0x02 ->
          throw new IllegalArgumentException("implement index interior");
      case 0x05 -> new TableInteriorPage(db, buf, base);
      case 0x0a -> throw new IllegalArgumentException("implement index leaf");
      case 0x0d -> new TableLeafPage(db, buf, base);
      default ->
          throw new DatabaseException("invalid page type: %x".formatted(first));
    };
  }

  protected Page(Database db, ByteBuffer buf, int base) {
    this.db = db;
    this.base = base;
    this.buf = buf;
    this.numCells = buf.position(base + 3).getShort();
  }

  protected abstract int headerSize();
  protected abstract int numRecords();

  protected abstract RecordType parseRecord(int index, ByteBuffer buf)
  throws DatabaseException;

  protected short cellOffset(int index) {
    return buf.position(base + headerSize() + index * 2).getShort();
  }

  // TODO: stream?
  public List<RecordType> records() throws DatabaseException {
    var records = new ArrayList<RecordType>();
    for (int i = 0; i < numRecords(); i++) records.add(parseRecord(i, buf));
    return records;
  }
}
