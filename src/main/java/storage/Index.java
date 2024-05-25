package storage;

import query.Value;
import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

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

  private record Key(List<Value> indexKey, long rowId) {}

  private Key parse(byte[] payload) throws StorageException {
    var record = Record.parse(payload, storage.getCharset());
    var rowId = record.values().removeLast();
    return new Key(record.values(), rowId.getInt());
  }

  private IndexRange.Endpoint<Key> parse(IndexRange.Endpoint<byte[]> raw)
  throws StorageException {
    return switch (raw) {
      case IndexRange.Unbounded<byte[]> ignored -> new IndexRange.Unbounded<>();
      case IndexRange.Bounded<byte[]> e ->
          new IndexRange.Bounded<>(parse(e.endpoint()));
    };
  }

  private IndexRange<Key> parse(IndexRange<byte[]> raw)
  throws StorageException {
    return new IndexRange<>(parse(raw.left()), parse(raw.right()),
                            raw.pageNumber());
  }

  // TODO: move this into IndexedPage and make its generic type Comparable
  private static boolean contains(IndexRange<Key> page, Value value) {
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

  void collect(Page.IndexPage page, HashSet<Long> rows, Value filter)
  throws StorageException, IOException {
    switch (page) {
      case Page.IndexInteriorPage interior -> {
        for (var encodedIndexedPage : interior.records()) {
          var indexedPage = parse(encodedIndexedPage);
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
        for (var key : leaf.records()) {
          var k = parse(key);
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
