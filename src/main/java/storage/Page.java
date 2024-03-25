package storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Page {
    private final Database db;
    private final ByteBuffer buf;
    private final Header header;
    private final int base;

    public Header getHeader() {
        return header;
    }

    public static final class FormatException extends Exception {
        public FormatException(String message) {
            super(message);
        }
    }

    public Page(Database db, ByteBuffer buf, int base) throws FormatException {
        this.db = db;
        this.base = base;
        this.buf = buf;
        byte first = buf.position(base).get();
        var type = switch (first) {
            case 0x02 -> Type.InteriorIndex;
            case 0x05 -> Type.InteriorTable;
            case 0x0a -> Type.LeafIndex;
            case 0x0d -> Type.LeafTable;
            default -> throw new FormatException(
                    String.format("invalid page type: %x", first));
        };
        int headerSize = switch (type) {
            case InteriorIndex, InteriorTable -> 12;
            case LeafIndex, LeafTable -> 8;
        };
        short numCells = buf.position(base + 3).getShort();
        short contentOffset = buf.position(base + 5).getShort();
        this.header = new Header(headerSize, type, numCells,
                                 contentOffset == 0 ? 65536 : contentOffset);
    }

    // TODO: stream?
    public List<TableLeafCell> readCells() {
        int pointerOffset = base + header.size;
        var cells = new ArrayList<TableLeafCell>();
        for (int i = 0; i < header.numCells; i++) {
            int cellOffset = buf.position(pointerOffset + i * 2).getShort();
            var payloadSize = VarInt.parseFrom(buf.position(cellOffset));
            cellOffset += payloadSize.size();
            var rowId = VarInt.parseFrom(buf.position(cellOffset));
            cellOffset += rowId.size();
            var payload = new byte[payloadSize.value()];
            buf.position(cellOffset).get(payload);
            cellOffset += payloadSize.value();
            Optional<Integer> overflowPage =
                    cellOffset == buf.limit() ? Optional.empty() : Optional.of(
                            buf.position(cellOffset).getInt());
            cells.add(new TableLeafCell(rowId.value(), payload, overflowPage));
        }
        return cells;
    }

    public List<Record> readRecords() throws Record.FormatException {
        var records = new ArrayList<Record>();
        for (var cell : readCells()) {
            records.add(Record.parse(db, cell.payload()));
        }
        return records;
    }

    public enum Type {
        InteriorIndex, InteriorTable, LeafIndex, LeafTable,
    }

    public record TableLeafCell(int rowId, byte[] payload, Optional<Integer> overflowPage) {
    }

    public record Header(int size, Type type, short numCells,
                         int contentOffset) {
    }
}
