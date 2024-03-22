import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    static void dbinfo(String path) {
        try {
            // TODO: avoid reading the whole file
            byte[] header = Files.readAllBytes(Path.of(path));
            short pageSize = ByteBuffer.wrap(header)
                                       .order(ByteOrder.BIG_ENDIAN)
                                       .position(16).getShort();
            System.out.printf("database page size: %d\n", pageSize);
        } catch (IOException e) {
            System.out.printf("error reading file: %s\n", e.getMessage());
        }
    }

    public static void main(String[] args) {
        // TODO: extract command line parsing
        if (args.length < 2) {
            System.err.printf("usage: sqlite3 <path> <command>\n");
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
