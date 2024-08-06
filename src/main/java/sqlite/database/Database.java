package sqlite.database;

import sqlite.query.QueryEngine;
import sqlite.query.Value;
import sqlite.sql.SQLException;
import sqlite.storage.BackingFile;
import sqlite.storage.StorageEngine;
import sqlite.storage.StorageException;
import sqlite.storage.Table;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class Database implements AutoCloseable {
  private static final System.Logger log = System.getLogger(
      Database.class.getCanonicalName());

  private final BackingFile f;

  private Database(SeekableByteChannel f) {
    this.f = new BackingFile(f);
  }

  private static void die(Exception e) {
    log.log(System.Logger.Level.ERROR, "sqlite: fatal error", e);
    System.exit(1);
  }

  public void close() {
    f.close();
  }

  private void dbinfo() {
    var storage = new StorageEngine(f);
    storage.getInfo().forEach(
        (field, val) -> System.out.printf("%s: %s\n", field, val));
  }

  private void tables() {
    var storage = new StorageEngine(f);
    var names = storage.getTables().stream().map(Table::name)
                       .filter(name -> !name.startsWith("sqlite_"))
                       .toList();
    System.out.println(String.join(" ", names));
  }

  private void schema() {
    var storage = new StorageEngine(f);
    for (var object : storage.getObjects()) {
      for (var entry : object.entrySet()) {
        System.out.printf(
            "%s: '%s'\n".formatted(entry.getKey(), entry.getValue()));
      }
      System.out.println();
    }
  }

  private void indices() {
    var storage = new StorageEngine(f);
    for (var index : storage.getIndices()) {
      System.out.printf("index: %s\n".formatted(index.name()));
      System.out.printf("table: %s\n".formatted(index.table().name()));
      System.out.printf(
          "fields: %s\n".formatted(index.definition().column()));
    }
  }

  private void query(String command)
  throws SQLException, IOException, StorageException {
    var storage = new StorageEngine(f);
    var query = new QueryEngine(storage);
    var results = query.evaluate(command);
    for (var row : results) {
      var values = row.columns().stream().map(Value::display).toList();
      System.out.println(String.join("|", values));
    }
  }

  private static void run(String path, String command) {
    try (var f = Files.newByteChannel(Path.of(path))) {
      var db = new Database(f);
      switch (command) {
        case ".dbinfo" -> db.dbinfo();
        case ".tables" -> db.tables();
        case ".indices" -> db.indices();
        case ".schema" -> db.schema();
        default -> db.query(command);
      }
    } catch (Exception e) {
      die(e);
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("usage: sqlite3 <path> <command>");
      System.exit(1);
    }
    String path = args[0];
    String command = args[1];
    run(path, command);
  }
}
