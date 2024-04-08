package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.io.IOException;
import java.util.ArrayList;
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

  // TODO: no
  void collect(IndexPage<?> page, List<Key> keys)
  throws DatabaseException, IOException {
    switch (page) {
      case IndexPage.Interior interior -> {
        for (var indexedPage : interior.records()) {
          collect(db.indexPage(indexedPage.pageNumber()), keys);
        }
      }
      case IndexPage.Leaf leaf -> {
        for (var key : leaf.records()) {
          keys.add(parse(key));
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
    var keys = new ArrayList<Key>();
    collect(root, keys);
    for (var key : keys) {
      if (key.indexKey.getFirst().equals(value)) {
        System.out.printf("found %s=%s: row id %d\n", column, value, key.rowId);
      }
    }
    return keys.stream().filter(key -> key.indexKey.getFirst().equals(value))
               .map(key -> key.rowId).toList();
  }

  private record Key(List<Value> indexKey, long rowId) {}
}
