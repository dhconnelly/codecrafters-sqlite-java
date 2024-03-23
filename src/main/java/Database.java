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
        var page = ByteBuffer.allocate(header.pageSize)
                             .order(ByteOrder.BIG_ENDIAN);
        int read = file.position(100 + (pageNumber - 1) * header.pageSize)
                       .read(page);
        if (read != header.pageSize) {
            throw new FormatException(
                    String.format("invalid page size: want %d, got %d",
                                  header.pageSize, read));
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
