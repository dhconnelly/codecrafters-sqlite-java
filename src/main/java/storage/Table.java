package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.io.IOException;
import java.util.*;

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
          : row.values().values().get(i);
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

  // TODO: move this into IndexedPage and make its generic type Comparable
  private static boolean contains(IndexedPage<Long> page, long rowId) {
    if (page.left() instanceof IndexedPage.Bounded<Long> left &&
        rowId < left.endpoint()) {
      return false;
    }
    if (page.right() instanceof IndexedPage.Bounded<Long> right &&
        rowId >= right.endpoint()) {
      return false;
    }
    return true;
  }

  private Optional<Row> lookup(TablePage<?> page, long rowId)
  throws DatabaseException, IOException {
    switch (page) {
      case TablePage.Interior interior -> {
        for (var child : interior.records()) {
          if (contains(child, rowId)) {
            return lookup(db.tablePage(child.pageNumber()), rowId);
          }
        }
      }
      case TablePage.Leaf leaf -> {
        for (var record : leaf.records()) {
          if (record.rowId() == rowId) return Optional.of(parseRow(record));
        }
      }
    }
    return Optional.empty();
  }

  public List<Row> rows() throws DatabaseException, IOException {
    var rows = new ArrayList<Row>();
    collect(root, rows);
    return rows;
  }

  public Optional<Row> get(long rowId) throws IOException, DatabaseException {
    return lookup(root, rowId);
  }

  public record Row(long rowId, Map<String, Value> values) {
    public Value get(String column) {return values.get(column);}
  }
}
