package storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public sealed abstract class Page<T> permits Page.LeafPage, Page.InteriorPage {
  protected final StorageEngine storage;
  private final ByteBuffer buf;
  private final int base;
  protected final short numCells;

  protected Page(StorageEngine storage, ByteBuffer buf, int base) {
    this.storage = storage;
    this.base = base;
    this.buf = buf;
    this.numCells = buf.position(base + 3).getShort();
  }

  public static sealed abstract class LeafPage<T> extends Page<T> permits TableLeafPage,
      IndexLeafPage {
    protected LeafPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
    }

    @Override
    protected int headerSize() {return 8;}

    @Override
    public int numRecords() {return numCells;}
  }

  public static sealed abstract class InteriorPage<T> extends Page<IndexRange<T>> permits TableInteriorPage,
      IndexInteriorPage {
    private final int rightPage;

    protected InteriorPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
      rightPage = buf.position(base + 8).getInt();
    }

    @Override
    protected int headerSize() {return 12;}

    @Override
    public int numRecords() {return numCells + 1;}

    protected static final class Cell<T> {
      public final int cellId;
      public final T payload;

      Cell(int cellId, T payload) {
        this.cellId = cellId;
        this.payload = payload;
      }
    }

    protected abstract Cell<T> parseCell(int index, ByteBuffer buf);

    @Override
    protected IndexRange<T> parseRecord(int index, ByteBuffer buf) {
      if (index == 0) {
        var cell = parseCell(index, buf);
        return new IndexRange<>(new IndexRange.Unbounded<>(),
                                new IndexRange.Bounded<>(cell.payload),
                                cell.cellId);
      } else if (index == numCells) {
        var cell = parseCell(index - 1, buf);
        return new IndexRange<>(new IndexRange.Bounded<>(cell.payload),
                                new IndexRange.Unbounded<>(),
                                rightPage);
      } else {
        var prev = parseCell(index - 1, buf);
        var cur = parseCell(index, buf);
        return new IndexRange<>(new IndexRange.Bounded<>(prev.payload),
                                new IndexRange.Bounded<>(cur.payload),
                                cur.cellId);
      }
    }
  }

  public static final class TableLeafPage extends LeafPage<Row> implements TablePage {
    TableLeafPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
    }

    @Override
    protected Row parseRecord(int index, ByteBuffer buf)
    throws StorageException {
      int offset = cellOffset(index);
      var payloadSize = VarInt.parseFrom(buf.position(offset));
      offset += payloadSize.size();
      var rowId = VarInt.parseFrom(buf.position(offset));
      offset += rowId.size();
      var payload = new byte[(int) payloadSize.value()];
      buf.position(offset).get(payload);
      // TODO: overflow pages
      return new Row(rowId.value(),
                     Record.parse(payload, storage.getCharset()));
    }
  }

  public static final class TableInteriorPage extends InteriorPage<Long> implements TablePage {
    TableInteriorPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
    }

    @Override
    protected Cell<Long> parseCell(int index, ByteBuffer buf) {
      if (index >= numCells) throw new AssertionError("index < numCells");
      int offset = cellOffset(index);
      int pageNumber = buf.position(offset).getInt();
      var rowId = VarInt.parseFrom(buf.position(offset + 4));
      return new Cell<>(pageNumber, rowId.value());
    }
  }

  public static final class IndexLeafPage extends LeafPage<byte[]> implements IndexPage {
    IndexLeafPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
    }

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

  public static final class IndexInteriorPage extends InteriorPage<byte[]> implements IndexPage {
    IndexInteriorPage(StorageEngine storage, ByteBuffer buf, int base) {
      super(storage, buf, base);
    }

    @Override
    protected Cell<byte[]> parseCell(int index, ByteBuffer buf) {
      if (index >= numCells) throw new AssertionError("index < numCells");
      int offset = cellOffset(index);
      int pageNumber = buf.position(offset).getInt();
      offset += 4;
      var payloadSize = VarInt.parseFrom(buf.position(offset));
      offset += payloadSize.size();
      var payload = new byte[(int) payloadSize.value()];
      buf.position(offset).get(payload);
      return new Cell<>(pageNumber, payload);
    }
  }

  public sealed interface TablePage permits TableLeafPage,
      TableInteriorPage {}
  public sealed interface IndexPage permits IndexLeafPage,
      IndexInteriorPage {}

  public TablePage asTablePage() throws StorageException {
    if (this instanceof TablePage page) {
      return page;
    }
    throw new StorageException(
        "wanted table page, got %s".formatted(this.getClass()));
  }

  public IndexPage asIndexPage() throws StorageException {
    if (this instanceof IndexPage page) {
      return page;
    }
    throw new StorageException(
        "wanted index page, got %s".formatted(this.getClass()));
  }

  public static Page<?> create(StorageEngine storage, ByteBuffer buf, int base)
  throws StorageException {
    byte first = buf.position(base).get();
    return switch (first) {
      case 0x02 -> new IndexInteriorPage(storage, buf, base);
      case 0x05 -> new TableInteriorPage(storage, buf, base);
      case 0x0a -> new IndexLeafPage(storage, buf, base);
      case 0x0d -> new TableLeafPage(storage, buf, base);
      default ->
          throw new StorageException("invalid page type: %x".formatted(first));
    };
  }

  protected abstract int headerSize();
  public abstract int numRecords();

  protected abstract T parseRecord(int index, ByteBuffer buf)
  throws StorageException;

  protected short cellOffset(int index) {
    return buf.position(base + headerSize() + index * 2).getShort();
  }

  // TODO: stream?
  List<T> records() throws StorageException {
    var records = new ArrayList<T>();
    for (int i = 0; i < numRecords(); i++) records.add(parseRecord(i, buf));
    return records;
  }

  public record Row(long rowId, Record values) {}
}
