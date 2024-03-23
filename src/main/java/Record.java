import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private List<Value> values;

    public Record(List<Value> values) {
        this.values = values;
    }

    public static Record parse(byte[] payload) throws FormatException {
        var values = new ArrayList<Value>();
        ByteBuffer buf = ByteBuffer.wrap(payload);
        var headerSize = VarInt.parseFrom(buf.position(0));
        int headerOffset = headerSize.size();
        int contentOffset = headerSize.value();
        while (headerOffset < headerSize.value()) {
            var serialType = VarInt.parseFrom(buf.position(headerOffset));
            headerOffset += serialType.size();
            int n = serialType.value();
            contentOffset += switch (n) {
                case 0 -> {
                    values.add(new NullValue());
                    yield 0;
                }
                case 1 -> {
                    values.add(new IntValue(buf.position(contentOffset).get()));
                    yield 1;
                }
                case 2 -> {
                    values.add(new IntValue(buf.position(contentOffset).getShort()));
                    yield 2;
                }
                case 4 -> {
                    values.add(new IntValue(buf.position(contentOffset).getInt()));
                    yield 4;
                }
                default -> {
                    if (n < 12) {
                        throw new FormatException("invalid serial type: %d".formatted(n));
                    }
                    if (n % 2 == 0) {
                        var blob = new byte[(n - 12) / 2];
                        buf.position(contentOffset).get(blob);
                        values.add(new BlobValue(blob));
                        yield (n - 12) / 2;
                    } else {
                        var data = new byte[(n - 13) / 2];
                        buf.position(contentOffset).get(data);
                        values.add(new StringValue(data));
                        yield (n - 13) / 2;
                    }
                }
            };
        }
        return new Record(values);
    }

    public List<Value> getValues() {
        return values;
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }

    public sealed interface Value permits NullValue, IntValue, FloatValue, BlobValue, StringValue {}
    public record NullValue() implements Value {}
    public record IntValue(int value) implements Value {}
    public record FloatValue(double value) implements Value {}
    public record BlobValue(byte[] blob) implements Value {}
    public record StringValue(byte[] data) implements Value {
        String decode(Database.TextEncoding encoding) throws UnsupportedEncodingException {
            return switch (encoding) {
                case Utf8 -> new String(data, "UTF-8");
                case Utf16le -> new String(data, "UTF-16LE");
                case Utf16be -> new String(data, "UTF-16BE");
            };
        }
    }
}
