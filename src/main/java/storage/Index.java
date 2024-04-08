package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class Index {
  private final Database db;
  private final String name;
  private final Table table;
  private final IndexPage<?> root;
  private final AST.CreateIndexStatement definition;

  public Index(Database db, String name, Table table, IndexPage<?> root,
               String schema) throws SQLException {
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

  // TODO: push down the filter and avoid a full scan here
  void collect(IndexPage<?> page, HashSet<Long> rows, Value filter)
  throws DatabaseException, IOException {
    switch (page) {
      case IndexPage.Interior interior -> {
        for (var indexedPage : interior.records()) {
          if (indexedPage.left() instanceof IndexedPage.Bounded<byte[]> key) {
            var k = parse(key.endpoint());
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          }
          if (indexedPage.right() instanceof IndexedPage.Bounded<byte[]> key) {
            var k = parse(key.endpoint());
            if (k.indexKey.getFirst().equals(filter)) rows.add(k.rowId);
          }
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
