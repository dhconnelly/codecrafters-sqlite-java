import sql.Evaluator;
import storage.Database;
import storage.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BiConsumer;

public class Main {
  @SuppressWarnings("ThrowablePrintedToSystemOut")
  private static void die(Exception e) {
    System.err.println(e);
    System.exit(1);
  }

  private static Database loadDB(String path) throws IOException,
                                                     Database.FormatException {
    return new Database(Files.newByteChannel(Path.of(path)));
  }

  private static void dbinfo(String path) {
    try {
      var db = loadDB(path);
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
    try {
      var names = loadDB(path).tables().stream().map(Table::name)
                              .filter(name -> !name.startsWith("sqlite_"))
                              .toList();
      System.out.println(String.join(" ", names));
    } catch (Exception e) {
      die(e);
    }
  }

  private static void schema(String path) {
    try {
      for (var table : loadDB(path).tables()) {
        System.out.printf("table: '%s'\n".formatted(table.name()));
        System.out.printf("type: %s\n".formatted(table.type()));
        System.out.printf("schema: '%s'\n".formatted(table.schema()));
        System.out.println();
      }
    } catch (Exception e) {
      die(e);
    }
  }

  private static void run(String path, String command) {
    try {
      var db = loadDB(path);
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
      case ".schema" -> schema(path);
      default -> run(path, command);
    }
  }
}
