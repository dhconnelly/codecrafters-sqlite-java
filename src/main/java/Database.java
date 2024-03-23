import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

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
        int to = pageNumber * header.pageSize;
        int from = to - header.pageSize + (pageNumber == 1 ? 100 : 0);
        var page = ByteBuffer.allocate(to - from).order(ByteOrder.BIG_ENDIAN);
        int read = file.position(from).read(page);
        if (read != page.capacity()) {
            throw new FormatException(
                    "invalid size for page %d: want %d, got %d".formatted(
                            pageNumber, page.capacity(), read));
        }
        return new Page(page);
    }

    public int numTables() throws IOException, FormatException,
                                  Page.FormatException {
        var schema = readPage(1);
        System.out.printf("schema page type: %s\n", schema.getType());
        return 0;
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
        return new Header(pageSize, pageCount);
    }

    public record Header(short pageSize, int pageCount) {
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }
}
