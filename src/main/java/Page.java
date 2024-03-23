import java.nio.ByteBuffer;

public class Page {
    private final ByteBuffer buf;
    private final int headerSize;
    private final Type type;

    public Type getType() {
        return type;
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }

    public Page(ByteBuffer buf) throws FormatException {
        this.buf = buf;
        byte first = buf.position(0).get();
        this.type = switch (first) {
            case 0x02 -> Type.InteriorIndex;
            case 0x05 -> Type.InteriorTable;
            case 0x0a -> Type.LeafIndex;
            case 0x0d -> Type.LeafTable;
            default -> throw new FormatException(
                    String.format("invalid page type: %x", first));
        };
        this.headerSize = switch (type) {
            case InteriorIndex, InteriorTable -> 12;
            case LeafIndex, LeafTable -> 8;
        };
    }

    public enum Type {
        InteriorIndex, InteriorTable, LeafIndex, LeafTable,
    }
}
