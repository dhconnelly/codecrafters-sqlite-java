package storage;

import sql.Value;

import java.io.IOException;
import java.util.List;

public class Table {
    private final Database db;
    private final Record schema;

    public Table(Database db, Record schema) {
        this.db = db;
        this.schema = schema;
    }

    public String getName() {
        return ((Value.StringValue) schema.valueAt(1)).data();
    }

    private int rootPage() {
        return ((Value.IntValue) schema.valueAt(3)).value();
    }

    public List<Record> rows() throws IOException, Database.FormatException, Page.FormatException,
                                      Record.FormatException {
        return db.readPage(rootPage()).readRecords();
    }
}
