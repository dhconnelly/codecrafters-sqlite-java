package storage;

import sql.SQLException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

public class Database implements AutoCloseable {
  private static final String SCHEMA = """
          CREATE TABLE sqlite_schema(
            type text,
            name text,
            tbl_name text,
            rootpage integer,
            sql text
          )
      """;
  private final SeekableByteChannel file;
  private final int pageSize;
  private final int pageCount;
  private final TextEncoding encoding;

  public Database(SeekableByteChannel file)
  throws IOException, DatabaseException {
    this.file = file;
    var header = Header.read(file);
    this.pageSize = header.pageSize;
    this.pageCount = header.pageCount;
    this.encoding = header.encoding;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  public Page<?> readPage(int pageNumber)
  throws IOException, DatabaseException {
    var page = ByteBuffer.allocate(pageSize).order(ByteOrder.BIG_ENDIAN);
    long offset = (long) (pageNumber - 1) * pageSize;
    int read = file.position(offset).read(page);
    if (read != page.capacity()) {
      throw new DatabaseException(
          "bad page size: want %d, got %d".formatted(page.capacity(), read));
    }
    return Page.create(this, page, pageNumber == 1 ? 100 : 0);
  }

  public Table schema() throws IOException, SQLException, DatabaseException {
    return new Table(this, "sqlite_schema", "sqlite_schema", "table",
                     readPage(1), SCHEMA);
  }

  public List<Map<String, String>> objects()
  throws SQLException, IOException, DatabaseException {
    var objects = new ArrayList<Map<String, String>>();
    for (var r : schema().rows()) {
      var object = new HashMap<String, String>();
      for (var col : List.of("name", "tbl_name", "type", "rootpage", "sql")) {
        object.put(col, "%s".formatted(r.get(col).display()));
      }
      objects.add(object);
    }
    return objects;
  }

  public List<Table> tables()
  throws IOException, SQLException, DatabaseException {
    var tables = new ArrayList<Table>();
    for (var r : schema().rows()) {
      if (r.get("type").getString().equals("table")) {
        tables.add(new Table(this, r.get("name").getString(),
                             r.get("tbl_name").getString(),
                             r.get("type").getString(),
                             readPage(r.get("rootpage").getInt()),
                             r.get("sql").getString()));
      }
    }
    return tables;
  }

  public Optional<Table> getTable(String name)
  throws IOException, SQLException, DatabaseException {
    return tables().stream().filter(t -> t.name().equals(name)).findFirst();
  }

  public String charset() {
    return switch (encoding) {
      case Utf16be -> "UTF-16BE";
      case Utf16le -> "UTF-16LE";
      case Utf8 -> "UTF-8";
    };
  }

  public int pageSize() {return pageSize;}

  public int pageCount() {return pageCount;}

  private enum TextEncoding {Utf8, Utf16le, Utf16be}

  private record Header(int pageSize, int pageCount, TextEncoding encoding) {
    static Header read(SeekableByteChannel file)
    throws IOException, DatabaseException {
      var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
      if (file.position(0).read(bytes) != 100) {
        throw new DatabaseException("invalid header: must contain 100 bytes");
      }
      int pageSize = Short.toUnsignedInt(bytes.position(16).getShort());
      int pageCount = bytes.position(28).getInt();
      int encoding = bytes.position(56).getInt();
      TextEncoding textEncoding = switch (encoding) {
        case 1 -> TextEncoding.Utf8;
        case 2 -> TextEncoding.Utf16le;
        case 3 -> TextEncoding.Utf16be;
        default ->
            throw new DatabaseException("bad encoding: %d".formatted(encoding));
      };
      return new Header(pageSize, pageCount, textEncoding);
    }
  }
}
