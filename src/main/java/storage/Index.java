package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public class Index {
  private final Database db;
  private final String name;
  private final Table table;
  private final IndexPage<?> root;
  private final AST.CreateIndexStatement definition;

  public Index(Database db, String name, Table table, IndexPage<?> root,
               String schema)
  throws SQLException {
    this.db = db;
    this.name = name;
    this.table = table;
    this.root = root;
    this.definition = new Parser(new Scanner(schema)).createIndex();
  }

  public String name() {return name;}

  public Table table() {return table;}

  public AST.CreateIndexStatement definition() {return definition;}

  private Key parse(byte[] payload) throws DatabaseException {
    var record = Record.parse(db, payload);
    var rowId = record.values().removeLast();
    return new Key(record.values(), rowId.getInt());
  }

  private IndexedPage.Endpoint<Key> parse(IndexedPage.Endpoint<byte[]> raw)
  throws DatabaseException {
    return switch (raw) {
      case IndexedPage.Unbounded<byte[]> ignored ->
          new IndexedPage.Unbounded<>();
      case IndexedPage.Bounded<byte[]> e ->
          new IndexedPage.Bounded<>(parse(e.endpoint()));
    };
  }

  private IndexedPage<Key> parse(IndexedPage<byte[]> raw)
  throws DatabaseException {
    return new IndexedPage<>(parse(raw.left()), parse(raw.right()),
                             raw.pageNumber());
  }

  // TODO: move this into IndexedPage and make its generic type Comparable
  private static boolean contains(IndexedPage<Key> page, Value value) {
    // TODO: handle different collating functions
    Optional<Value> left = page.left().get()
                               .map(key -> key.indexKey.getFirst());
    Optional<Value> right = page.right().get()
                                .map(key -> key.indexKey.getFirst());
    if (left.isPresent() && left.get().compareTo(value) > 0) {
      return false;
    }
    if (right.isPresent() && right.get().compareTo(value) < 0) {
      return false;
    }
    return true;
  }

  void collect(IndexPage<?> page, HashSet<Long> rows, Value filter)
  throws DatabaseException, IOException {
    switch (page) {
      case IndexPage.Interior interior -> {
        for (var encodedIndexedPage : interior.records()) {
          var indexedPage = parse(encodedIndexedPage);
          if (!contains(indexedPage, filter)) continue;
          indexedPage.left().get().ifPresent(k -> {
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          });
          indexedPage.right().get().ifPresent(k -> {
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          });
          collect(db.indexPage(indexedPage.pageNumber()), rows, filter);
        }
      }
      case IndexPage.Leaf leaf -> {
        for (var key : leaf.records()) {
          var k = parse(key);
          if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
        }
      }
    }
  }

  public List<Long> find(String column, Value value)
  throws SQLException, IOException, DatabaseException {
    if (!definition.column().equals(column)) {
      throw new SQLException(
          "index %s does not cover column %s".formatted(name, column));
    }
    var rows = new HashSet<Long>();
    collect(root, rows, value);
    return rows.stream().toList();
  }

  private record Key(List<Value> indexKey, long rowId) {}
}
