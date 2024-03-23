import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class Main {
    static void dbinfo(String path) {
        try {
            var db = new Database(Files.newByteChannel(Path.of(path)));
            var header = db.getHeader();
            System.out.printf("%-20s %d\n", "database page size:", header.pageSize());
            System.out.printf("%-20s %d\n", "database page count:", header.pageCount());
            var tables = db.getTables();
            System.out.printf("%-20s %d\n", "number of tables:", tables.size());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    static void tables(String path) {
        try {
            var db = new Database(Files.newByteChannel(Path.of(path)));
            var names = new ArrayList<String>();
            for (var table : db.getTables()) {
                if (!table.getName().startsWith("sqlite_")) names.add(table.getName());
            }
            System.out.println(String.join(" ", names));
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        // TODO: extract command line parsing
        if (args.length < 2) {
            System.err.println("usage: sqlite3 <path> <command>");
            System.exit(1);
        }
        String path = args[0];
        String command = args[1];

        // TODO: extract statement parsing
        switch (command) {
            case ".dbinfo" -> dbinfo(path);
            case ".tables" -> tables(path);
            default -> System.out.printf("invalid command: %s\n", command);
        }
    }
}
