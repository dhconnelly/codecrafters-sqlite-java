package storage;

import sql.Value;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record {
    private final Database db;

    private List<Value> values;

    public Record(Database db, List<Value> values) {
        this.db = db;
        this.values = values;
    }

    public static Record parse(Database db, byte[] payload) throws FormatException {
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
                    values.add(new Value.NullValue());
                    yield 0;
                }
                case 1 -> {
                    values.add(new Value.IntValue(buf.position(contentOffset).get()));
                    yield 1;
                }
                case 2 -> {
                    values.add(new Value.IntValue(buf.position(contentOffset).getShort()));
                    yield 2;
                }
                case 4 -> {
                    values.add(new Value.IntValue(buf.position(contentOffset).getInt()));
                    yield 4;
                }
                default -> {
                    if (n < 12) {
                        throw new FormatException("invalid serial type: %d".formatted(n));
                    }
                    if (n % 2 == 0) {
                        var blob = new byte[(n - 12) / 2];
                        buf.position(contentOffset).get(blob);
                        values.add(new Value.BlobValue(blob));
                        yield (n - 12) / 2;
                    } else {
                        var data = new byte[(n - 13) / 2];
                        buf.position(contentOffset).get(data);
                        String charset = db.getEncoding();
                        try {
                            values.add(new Value.StringValue(new String(data, charset)));
                        } catch (UnsupportedEncodingException e) {
                            throw new AssertionError("invalid charset: " + charset);
                        }
                        yield (n - 13) / 2;
                    }
                }
            };
        }
        return new Record(db, values);
    }

    public List<Value> getValues() {
        return values;
    }

    public Value valueAt(int index) {
        return values.get(index);
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }
}
