package sqlite.storage;

import sqlite.query.Value;
import sqlite.sql.AST;
import sqlite.sql.Parser;
import sqlite.sql.SQLException;
import sqlite.sql.Scanner;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class Index {
  private final StorageEngine storage;
  private final String name;
  private final Table table;
  private final Page.IndexPage root;
  private final AST.CreateIndexStatement definition;

  public Index(StorageEngine storage, String name, Table table,
               Page.IndexPage root,
               String schema)
  throws SQLException {
    this.storage = storage;
    this.name = name;
    this.table = table;
    this.root = root;
    this.definition = new Parser(new Scanner(schema)).createIndex();
  }

  public record Key(List<Value> indexKey, long rowId) {}

  // TODO: move this into IndexedPage and make its generic type Comparable
  private static boolean contains(Pointer<Key> page, Value value) {
    // TODO: handle different collating functions
    Optional<Value> left = page.left().get()
                               .map(key -> key.indexKey.getFirst());
    Optional<Value> right = page.right().get()
                                .map(key -> key.indexKey.getFirst());
    if (left.isPresent() && left.get().compareTo(value) > 0) {
      return false;
    }
    return right.isEmpty() || right.get().compareTo(value) >= 0;
  }

  void collect(Page.IndexPage page, HashSet<Long> rows, Value filter)
  throws StorageException, IOException {
    switch (page) {
      case Page.IndexInteriorPage interior -> {
        for (var indexedPage : interior.recordsIterable()) {
          if (!contains(indexedPage, filter)) continue;
          indexedPage.left().get().ifPresent(k -> {
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          });
          indexedPage.right().get().ifPresent(k -> {
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          });
          collect(storage.getPage(indexedPage.pageNumber()).asIndexPage(), rows,
                  filter);
        }
      }
      case Page.IndexLeafPage leaf -> {
        for (var k : leaf.recordsIterable()) {
          if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
        }
      }
    }
  }

  public String name() {return name;}

  public Table table() {return table;}

  // TODO: return a string
  public AST.CreateIndexStatement definition() {return definition;}

  public List<Long> find(String column, Value value)
  throws SQLException, IOException, StorageException {
    if (!definition.column().equals(column)) {
      throw new SQLException(
          "index %s does not cover column %s".formatted(name, column));
    }
    var rows = new HashSet<Long>();
    collect(root, rows, value);
    return rows.stream().toList();
  }

}
