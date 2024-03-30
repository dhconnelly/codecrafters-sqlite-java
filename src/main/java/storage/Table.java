package storage;

import sql.Parser;
import sql.Scanner;
import sql.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// TODO: this should be able to represent the sqlite_schema table too
public class Table {
  private final Database db;
  private final Record metadata;
  private final Map<String, Integer> columns;

  public Table(Database db, Record schema) throws Parser.Error, Scanner.Error {
    this.db = db;
    this.metadata = schema;
    this.columns = new HashMap<>();
    var definition = new Parser(
        new Scanner(schema.get(4).display())).createTable();
    for (int i = 0; i < definition.columns().size(); i++) {
      columns.put(definition.columns().get(i).name(), i);
    }
  }

  public Optional<Integer> getIndexForColumn(String columnName) {
    return Optional.ofNullable(columns.get(columnName));
  }

  public String name() {
    return ((Value.StringValue) metadata.get(1)).data();
  }

  public String type() {
    return ((Value.StringValue) metadata.get(0)).data();
  }

  public int rootPage() {
    return ((Value.IntValue) metadata.get(3)).value();
  }

  public String schema() {
    return ((Value.StringValue) metadata.get(4)).data();
  }

  public List<Record> rows() throws IOException, Database.FormatException,
                                    Page.FormatException,
                                    Record.FormatException {
    return db.readPage(rootPage()).readRecords();
  }
}
