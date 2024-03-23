import java.io.UnsupportedEncodingException;

public class Table {
    private final Database db;
    private final Record schema;

    public Table(Database db, Record schema) {
        this.db = db;
        this.schema = schema;
    }

    public String getName() throws UnsupportedEncodingException {
        Record.StringValue name = (Record.StringValue) schema.getValues().get(1);
        return name.decode(db.getHeader().encoding());
    }
}
