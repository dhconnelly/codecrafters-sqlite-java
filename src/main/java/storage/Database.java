package storage;

import sql.Parser;
import sql.Scanner;
import sql.Value;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Database {

  private final SeekableByteChannel file;
  private final Header header;

  public Database(SeekableByteChannel file) throws IOException,
                                                   FormatException {
    this.file = file;
    this.header = readHeader();
  }

  public Header getHeader() {
    return header;
  }

  public Page readPage(int pageNumber) throws IOException, FormatException,
                                              Page.FormatException {
    var page = ByteBuffer.allocate(header.pageSize).order(ByteOrder.BIG_ENDIAN);
    int read = file.position((pageNumber - 1) * header.pageSize).read(page);
    if (read != page.capacity()) {
      throw new FormatException(
          "invalid page size: want %d, got %d".formatted(page.capacity(),
                                                         read));
    }
    return new Page(this, page, pageNumber == 1 ? 100 : 0);
  }

  public List<Record> getSchema() throws IOException, FormatException,
                                         Page.FormatException,
                                         Record.FormatException {
    return readPage(1).readRecords();
  }

  public List<Table> getTables() throws IOException, FormatException,
                                        Page.FormatException,
                                        Record.FormatException, Parser.Error,
                                        Scanner.Error {
    var tables = new ArrayList<Table>();
    for (Record r : getSchema()) {
      var values = r.getValues();
      var value = (Value.StringValue) values.getFirst();
      var type = value.data();
      if (type.equals("table")) tables.add(new Table(this, r));
    }
    return tables;
  }

  public Optional<Table> getTable(String name) throws IOException,
                                                      FormatException,
                                                      Page.FormatException,
                                                      Record.FormatException,
                                                      Parser.Error,
                                                      Scanner.Error {
    return getTables().stream().filter((table) -> table.getName().equals(name))
                      .findFirst();
  }

  private Header readHeader() throws IOException, FormatException {
    var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
    int read = file.position(0).read(bytes);
    if (read != 100) {
      throw new FormatException("invalid header: must contain 100" + " bytes");
    }
    int pageSize = Short.toUnsignedInt(bytes.position(16).getShort());
    int pageCount = bytes.position(28).getInt();
    int n = bytes.position(56).getInt();
    TextEncoding encoding = switch (n) {
      case 1 -> TextEncoding.Utf8;
      case 2 -> TextEncoding.Utf16le;
      case 3 -> TextEncoding.Utf16be;
      default ->
          throw new FormatException("invalid text encoding: %d".formatted(n));
    };
    return new Header(pageSize, pageCount, encoding);
  }

  public String getEncoding() {
    return switch (header.encoding) {
      case Utf16be -> "UTF-16BE";
      case Utf16le -> "UTF-16LE";
      case Utf8 -> "UTF-8";
    };
  }

  public enum TextEncoding {
    Utf8, Utf16le, Utf16be
  }

  public record Header(int pageSize, int pageCount, TextEncoding encoding) {}

  public static final class FormatException extends Exception {
    public FormatException(String message) {
      super(message);
    }
  }
}
