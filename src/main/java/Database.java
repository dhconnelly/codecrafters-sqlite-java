import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

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

    public Page readPage(int pageNumber) throws IOException,
                                                FormatException,
                                                Page.FormatException {
        var page = ByteBuffer.allocate(header.pageSize).order(ByteOrder.BIG_ENDIAN);
        int read = file.position((pageNumber - 1) * header.pageSize).read(page);
        if (read != page.capacity()) {
            throw new FormatException(
                    "invalid page size: want %d, got %d".formatted(page.capacity(), read));
        }
        return new Page(page, pageNumber == 1 ? 100 : 0);
    }

    public List<Table> getTables() throws IOException, FormatException, Page.FormatException,
                                          Record.FormatException {
        var schema = readPage(1);
        var tables = new ArrayList<Table>();
        for (Record r : schema.readRecords()) {
            var values = r.getValues();
            var value = (Record.StringValue) values.getFirst();
            var type = value.decode(header.encoding);
            if (type.equals("table")) tables.add(new Table(this, r));
        }
        return tables;
    }

    private Header readHeader() throws IOException, FormatException {
        var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
        int read = file.position(0).read(bytes);
        if (read != 100) {
            throw new FormatException("invalid header: must contain 100" +
                                      " bytes");
        }
        short pageSize = bytes.position(16).getShort();
        int pageCount = bytes.position(28).getInt();
        int n = bytes.position(56).getInt();
        TextEncoding encoding = switch (n) {
            case 1 -> TextEncoding.Utf8;
            case 2 -> TextEncoding.Utf16le;
            case 3 -> TextEncoding.Utf16be;
            default -> throw new FormatException("invalid text encoding: %d".formatted(n));
        };
        return new Header(pageSize, pageCount, encoding);
    }

    public enum TextEncoding {
        Utf8, Utf16le, Utf16be
    }

    public record Header(short pageSize, int pageCount, TextEncoding encoding) {
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }
}
