package storage;

import sql.SQLException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class StorageEngine {
  private static final String SCHEMA = """
          CREATE TABLE sqlite_schema(
            type text,
            name text,
            tbl_name text,
            rootpage integer,
            sql text
          )
      """;

  private final int pageSize;
  private final SeekableByteChannel file;
  private final Charset charset;

  public StorageEngine(SeekableByteChannel file)
  throws IOException, StorageException {
    var header = Header.read(file);
    this.pageSize = header.pageSize;
    this.file = file;
    this.charset = switch (header.encoding) {
      case Utf16be -> StandardCharsets.UTF_16BE;
      case Utf16le -> StandardCharsets.UTF_16LE;
      case Utf8 -> StandardCharsets.UTF_8;
    };
  }

  public Map<String, Object> getInfo()
  throws SQLException, IOException, StorageException {
    return Map.of(
        "database page size", pageSize,
        "number of tables", getTables().size()
    );
  }

  private Table schema() throws IOException, SQLException, StorageException {
    return new Table(this, "sqlite_schema", getPage(1).asTablePage(),
                     SCHEMA);
  }

  public List<Map<String, String>> getObjects()
  throws SQLException, IOException, StorageException {
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

  public List<Index> getIndices()
  throws IOException, StorageException, SQLException {
    var indices = new ArrayList<Index>();
    for (var r : schema().rows()) {
      if (r.get("type").getString().equals("index")) {
        var name = r.get("name").getString();
        var tableName = r.get("tbl_name").getString();
        var table = getTable(tableName).orElseThrow(() -> new StorageException(
            "index %s: table does not exist: %s".formatted(name, tableName)));
        indices.add(new Index(this, name, table,
                              getPage((int) r.get("rootpage").getInt())
                                  .asIndexPage(),
                              r.get("sql").getString()));
      }
    }
    return indices;
  }

  public List<Table> getTables()
  throws IOException, SQLException, StorageException {
    var tables = new ArrayList<Table>();
    for (var r : schema().rows()) {
      if (r.get("type").getString().equals("table")) {
        tables.add(new Table(this, r.get("name").getString(),
                             getPage((int) r.get("rootpage").getInt())
                                 .asTablePage(),
                             r.get("sql").getString()));
      }
    }
    return tables;
  }

  private Optional<Table> getTable(String name)
  throws IOException, SQLException, StorageException {
    return getTables().stream().filter(t -> t.name().equals(name)).findFirst();
  }

  private enum TextEncoding {Utf8, Utf16le, Utf16be}

  private record Header(int pageSize, int pageCount, TextEncoding encoding) {
    static Header read(SeekableByteChannel file)
    throws IOException, StorageException {
      var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
      if (file.position(0).read(bytes) != 100) {
        throw new StorageException("invalid header: must contain 100 bytes");
      }
      int pageSize = Short.toUnsignedInt(bytes.position(16).getShort());
      int pageCount = bytes.position(28).getInt();
      int encoding = bytes.position(56).getInt();
      TextEncoding textEncoding = switch (encoding) {
        case 1 -> TextEncoding.Utf8;
        case 2 -> TextEncoding.Utf16le;
        case 3 -> TextEncoding.Utf16be;
        default ->
            throw new StorageException("bad encoding: %d".formatted(encoding));
      };
      return new Header(pageSize, pageCount, textEncoding);
    }
  }

  Charset getCharset() {
    return charset;
  }

  Page<?> getPage(int pageNumber)
  throws IOException, StorageException {
    var page = ByteBuffer.allocate(pageSize).order(ByteOrder.BIG_ENDIAN);
    long offset = (long) (pageNumber - 1) * pageSize;
    int read = file.position(offset).read(page);
    if (read != page.capacity()) {
      throw new StorageException(
          "bad page size: want %d, got %d".formatted(page.capacity(), read));
    }
    return Page.create(this, page, pageNumber == 1 ? 100 : 0);
  }
}
