import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public class Database {

    private final SeekableByteChannel file;
    private final Header header;

    public Database(SeekableByteChannel file) throws IOException,
                                                     SQLiteFormatException {
        this.file = file;
        this.header = readHeader();
    }

    public Header getHeader() {
        return header;
    }

    private Header readHeader() throws IOException, SQLiteFormatException {
        var bytes = ByteBuffer.allocate(100).order(ByteOrder.BIG_ENDIAN);
        int read = this.file.position(0).read(bytes);
        if (read != 100) {
            throw new SQLiteFormatException("invalid header: must contain 100" +
                                            " bytes");
        }
        short pageSize = bytes.position(16).getShort();
        int pageCount = bytes.position(28).getInt();
        return new Header(pageSize, pageCount);
    }

    public record Header(short pageSize, int pageCount) {
    }

    public static class SQLiteFormatException extends Exception {
        public SQLiteFormatException(String message) {
            super(message);
        }
    }
}
