package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {
  private final Database db;
  private final String name;
  private final TablePage<?> root;
  private final AST.CreateTableStatement definition;

  public Table(Database db, String name, TablePage<?> root, String schema)
  throws SQLException {
    this.db = db;
    this.name = name;
    this.root = root;
    this.definition = new Parser(new Scanner(schema)).createTable();
  }

  private static boolean isIntegerPK(AST.ColumnDefinition col) {
    var mods = col.modifiers();
    return mods.contains("integer") && mods.contains("primary") &&
           mods.contains("key");
  }

  public String name() {return name;}

  private Row parseRow(TablePage.Leaf.Row row) {
    var record = new HashMap<String, Value>();
    for (int i = 0; i < definition.columns().size(); i++) {
      var col = definition.columns().get(i);
      var val = isIntegerPK(col)
          ? new Value.IntValue(row.rowId())
          : row.values().get(i);
      record.put(col.name(), val);
    }
    return new Row(row.rowId(), record);
  }

  private void collect(TablePage<?> page, List<Row> rows)
  throws DatabaseException, IOException {
    switch (page) {
      case TablePage.Leaf leaf ->
          leaf.records().stream().map(this::parseRow).forEach(rows::add);
      case TablePage.Interior interior -> {
        for (var indexedPage : interior.records()) {
          collect(db.tablePage(indexedPage.pageNumber()), rows);
        }
      }
    }
  }

  public List<Row> rows() throws DatabaseException, IOException {
    var rows = new ArrayList<Row>();
    collect(root, rows);
    return rows;
  }

  public record Row(int rowId, Map<String, Value> values) {
    public Value get(String column) {return values.get(column);}
  }
}
