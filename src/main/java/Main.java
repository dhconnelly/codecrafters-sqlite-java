import sql.Evaluator;
import storage.Database;
import storage.DatabaseException;
import storage.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;

public class Main {
  private static final System.Logger log = System.getLogger(
      Main.class.getCanonicalName());

  private static void die(Exception e) {
    log.log(System.Logger.Level.ERROR, "sqlite: fatal error", e);
    System.exit(1);
  }

  private static Database loadDB(String path)
  throws IOException, DatabaseException {
    return new Database(Files.newByteChannel(Path.of(path)));
  }

  private static void dbinfo(String path) {
    try (var db = loadDB(path)) {
      BiConsumer<String, Integer> display = (field, val) ->
          System.out.printf("%-20s %d\n", field, val);
      display.accept("database page size:", db.pageSize());
      display.accept("database page count:", db.pageCount());
      System.out.printf("%-20s %d\n", "number of tables:", db.tables().size());
    } catch (Exception e) {
      die(e);
    }
  }

  private static void tables(String path) {
    try (var db = loadDB(path)) {
      var names = db.tables().stream().map(Table::name)
                    .filter(name -> !name.startsWith("sqlite_"))
                    .toList();
      System.out.println(String.join(" ", names));
    } catch (Exception e) {
      die(e);
    }
  }

  private static void schema(String path) {
    try (var db = loadDB(path)) {
      for (var object : db.objects()) {
        for (var entry : object.entrySet()) {
          System.out.printf(
              "%s: '%s'\n".formatted(entry.getKey(), entry.getValue()));
        }
        System.out.println();
      }
    } catch (Exception e) {
      die(e);
    }
  }

  private static void indices(String path) {
    try (var db = loadDB(path)) {
      for (var index : db.indices()) {
        System.out.printf("index: %s\n".formatted(index.name()));
        System.out.printf("table: %s\n".formatted(index.table().name()));
        System.out.printf(
            "fields: %s\n".formatted(index.definition().columns()));
      }
    } catch (Exception e) {
      die(e);
    }
  }

  private static void run(String path, String command) {
    try (var db = loadDB(path)) {
      var vm = new Evaluator(db);
      vm.evaluate(command);
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
    switch (command) {
      case ".dbinfo" -> dbinfo(path);
      case ".tables" -> tables(path);
      case ".indices" -> indices(path);
      case ".schema" -> schema(path);
      default -> run(path, command);
    }
  }
}
