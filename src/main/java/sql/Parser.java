package sql;

import java.util.List;

public class Parser {
    private final Scanner scanner;

    public Parser(Scanner scanner) {
        this.scanner = scanner;
    }

    public static class Error extends Exception {
        public Error(String message) {
            super(message);
        }

        public static Error unexpectedEof() {return new Error("parser: unexpected eof");}
    }

    private Token eat(Token.Type type) throws Scanner.Error, Error {
        Token tok = scanner.next().orElseThrow(Error::unexpectedEof);
        if (tok.type() != type) {
            throw new Error("parser: want token %s, got %s".formatted(type, tok.type()));
        }
        return tok;
    }

    private AST.Expr expr() throws Scanner.Error, Error {
        Token tok = scanner.next().orElseThrow(Error::unexpectedEof);
        switch (tok.type()) {
            case Token.Type.STAR -> {
                return new AST.Star();
            }
            case Token.Type.IDENT -> {
                eat(Token.Type.LPAREN);
                var arg = this.expr();
                eat(Token.Type.RPAREN);
                return new AST.FnCall(tok.text(), List.of(arg));
            }
            default -> {
                throw new Error("parser: invalid expression: %s".formatted(tok.type()));
            }
        }
    }

    public AST.Statement statement() throws Scanner.Error, Error {
        eat(Token.Type.SELECT);
        var expr = this.expr();
        eat(Token.Type.FROM);
        var table = eat(Token.Type.IDENT);
        return new AST.SelectStatement(List.of(expr), table.text());
    }
}
