package sqlite.storage;

import sqlite.query.Value;
import sqlite.sql.AST;
import sqlite.sql.Parser;
import sqlite.sql.SQLException;
import sqlite.sql.Scanner;

import java.io.IOException;
import java.util.*;

public class Table {
  private final StorageEngine storage;
  private final String name;
  private final Page.TablePage root;
  private final AST.CreateTableStatement definition;

  public Table(StorageEngine storage, String name, Page.TablePage root,
               String schema)
  throws SQLException {
    this.storage = storage;
    this.name = name;
    this.root = root;
    this.definition = new Parser(new Scanner(schema)).createTable();
  }

  private static boolean isIntegerPK(AST.ColumnDefinition col) {
    var mods = col.modifiers();
    return mods.contains("integer") && mods.contains("primary") &&
           mods.contains("key");
  }


  private Row parseRow(Page.Row row) {
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

  private void collect(Page.TablePage page, List<Row> rows)
  throws StorageException, IOException {
    switch (page) {
      case Page.TableLeafPage leaf ->
          leaf.records().stream().map(this::parseRow).forEach(rows::add);
      case Page.TableInteriorPage interior -> {
        for (var indexedPage : interior.records()) {
          collect(storage.getPage(indexedPage.pageNumber()).asTablePage(),
                  rows);
        }
      }
    }
  }

  // TODO: move this into IndexedPage and make its generic type Comparable
  private static boolean contains(Pointer<Long> page, long rowId) {
    if (page.left() instanceof Pointer.Bounded<Long> left &&
        rowId < left.endpoint()) {
      return false;
    }
    if (page.right() instanceof Pointer.Bounded<Long> right &&
        rowId > right.endpoint()) {
      return false;
    }
    return true;
  }

  private Optional<Row> lookup(Page.TablePage page, long rowId)
  throws StorageException, IOException {
    switch (page) {
      case Page.TableInteriorPage interior -> {
        for (var child : interior.records()) {
          if (contains(child, rowId)) {
            return lookup(storage.getPage(child.pageNumber()).asTablePage(),
                          rowId);
          }
        }
      }
      case Page.TableLeafPage leaf -> {
        for (var record : leaf.records()) {
          if (record.rowId() == rowId) return Optional.of(parseRow(record));
        }
      }
    }
    return Optional.empty();
  }

  public record Row(long rowId, Map<String, Value> values) {
    public Value get(String column) {return values.get(column);}
  }

  public String name() {return name;}

  // TODO: stream
  public List<Row> rows() throws StorageException, IOException {
    var rows = new ArrayList<Row>();
    collect(root, rows);
    return rows;
  }

  public Optional<Row> get(long rowId) throws IOException, StorageException {
    return lookup(root, rowId);
  }
}
