package storage;

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
  private final SeekableByteChannel file;
  private final int pageSize;
  private final int pageCount;
  private final TextEncoding encoding;
  private final List<Table> tables;

  public Database(SeekableByteChannel file) throws IOException,
                                                   FormatException,
                                                   Page.FormatException,
                                                   Record.FormatException,
                                                   Parser.Error, Scanner.Error {
    this.file = file;
    var header = Header.read(file);
    this.pageSize = header.pageSize;
    this.pageCount = header.pageCount;
    this.encoding = header.encoding;
    this.tables = new ArrayList<>();
    for (Record r : readPage(1).readRecords()) {
      if (r.get(0).asString().orElseThrow().equals("table")) {
        tables.add(new Table(this, r));
      }
    }
  }

  public Page readPage(int pageNumber) throws IOException, FormatException,
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

  public Optional<Table> getTable(String name) {
    return tables.stream().filter((table) -> table.name().equals(name))
                 .findFirst();
  }

  public List<Table> tables() {return tables;}

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
