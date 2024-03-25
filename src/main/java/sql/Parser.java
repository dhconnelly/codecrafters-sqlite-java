package sql;

import java.util.Optional;

public class Parser {
    private final Scanner scanner;

    public Parser(Scanner scanner) {
        this.scanner = scanner;
    }

    public AST statement() throws Scanner.Error {
        for (Optional<Token> tok; (tok = scanner.next()).isPresent(); ) {
            System.out.println(tok);
        }
        return null;
    }
}
