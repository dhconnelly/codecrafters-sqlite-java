package storage;

import sql.AST;
import sql.Parser;
import sql.Scanner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Database {
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

  public Database(SeekableByteChannel file) throws IOException,
                                                   FormatException {
    this.file = file;
    var header = Header.read(file);
    this.pageSize = header.pageSize;
    this.pageCount = header.pageCount;
    this.encoding = header.encoding;
  }

  private static AST.CreateTableStatement parse(String schema) throws Parser.Error, Scanner.Error {
    return new Parser(new Scanner(schema)).createTable();
  }

  private Page readPage(int pageNumber) throws IOException, FormatException,
                                               Page.FormatException {
    var page = ByteBuffer.allocate(pageSize).order(ByteOrder.BIG_ENDIAN);
    long offset = (long) (pageNumber - 1) * pageSize;
    int read = file.position(offset).read(page);
    if (read != page.capacity()) {
      throw new FormatException(
          "bad page size: want %d, got %d".formatted(page.capacity(), read));
    }
    return new Page(this, page, pageNumber == 1 ? 100 : 0);
  }

  public Table schema() throws IOException, FormatException,
                               Page.FormatException, Parser.Error,
                               Scanner.Error {
    return new Table("sqlite_schema", "table", readPage(1), SCHEMA);
  }

  public List<Table> tables() throws IOException, FormatException,
                                     Page.FormatException,
                                     Record.FormatException, Parser.Error,
                                     Scanner.Error {
    var tables = new ArrayList<Table>();
    for (Record r : schema().rows()) {
      if (r.get(0).asString().orElseThrow().equals("table")) {
        tables.add(new Table(r.get(1).asString().orElseThrow(),
                             r.get(0).asString().orElseThrow(),
                             readPage(r.get(3).asInt().orElseThrow()),
                             r.get(4).asString().orElseThrow()));
      }
    }
    return tables;
  }

  public Optional<Table> getTable(String name) throws Parser.Error,
                                                      Scanner.Error,
                                                      IOException,
                                                      FormatException,
                                                      Page.FormatException,
                                                      Record.FormatException {
    return tables().stream().filter((table) -> table.name().equals(name))
                   .findFirst();
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
    static Header read(SeekableByteChannel file) throws IOException,
                                                        FormatException {
      var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
      if (file.position(0).read(bytes) != 100) {
        throw new FormatException(
            "invalid header: must contain 100" + " bytes");
      }
      int pageSize = Short.toUnsignedInt(bytes.position(16).getShort());
      int pageCount = bytes.position(28).getInt();
      int n = bytes.position(56).getInt();
      TextEncoding encoding = switch (n) {
        case 1 -> TextEncoding.Utf8;
        case 2 -> TextEncoding.Utf16le;
        case 3 -> TextEncoding.Utf16be;
        default -> throw new FormatException("bad encoding: %d".formatted(n));
      };
      return new Header(pageSize, pageCount, encoding);
    }
  }

  public static final class FormatException extends Exception {
    public FormatException(String message) {
      super(message);
    }
  }
}
