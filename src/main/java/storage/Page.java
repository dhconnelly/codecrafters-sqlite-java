package storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public sealed abstract class Page<RecordType> permits TableLeafPage {
  protected final Database db;
  private final ByteBuffer buf;
  private final int base;
  private final short numCells;

  public static Page<?> create(Database db, ByteBuffer buf, int base)
  throws DatabaseException {
    byte first = buf.position(base).get();
    return switch (first) {
      case 0x02 ->
          throw new IllegalArgumentException("implement index interior");
      case 0x05 ->
          throw new IllegalArgumentException("implement table interior");
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

  protected abstract RecordType parseRecord(ByteBuffer buf, int cellOffset)
  throws DatabaseException;

  // TODO: stream?
  public List<RecordType> records() throws DatabaseException {
    int pointerOffset = base + headerSize();
    var records = new ArrayList<RecordType>();
    for (int i = 0; i < numCells; i++) {
      int cellOffset = buf.position(pointerOffset + i * 2).getShort();
      records.add(parseRecord(buf, cellOffset));
    }
    return records;
  }
}
