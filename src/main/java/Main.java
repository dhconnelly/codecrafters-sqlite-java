import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    static void dbinfo(String path) {
        try {
            var db = new Database(Files.newByteChannel(Path.of(path)));
            var header = db.getHeader();
            System.out.printf("%-20s %d\n", "database page size:",
                              header.pageSize());
            System.out.printf("%-20s %d\n", "database page count:",
                              header.pageCount());
            System.out.printf("%-20s %d\n", "number of tables:",
                              db.numTables());
        } catch (Exception e) {
            System.out.printf("error reading file: %s\n", e.getMessage());
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
            default -> System.out.printf("invalid command: %s\n", command);
        }
    }
}
