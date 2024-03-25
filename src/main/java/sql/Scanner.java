package sql;

import java.util.Optional;

public class Scanner {
    private final String s;
    private int pos;

    public Scanner(String s) {
        this.s = s;
        this.pos = 0;
    }

    public static class Error extends Exception {
        public Error(String message) {
            super(message);
        }
    }

    private Token.Type getType(String text) {
        return switch (text.toLowerCase()) {
            case "select" -> Token.Type.SELECT;
            case "from" -> Token.Type.FROM;
            default -> Token.Type.IDENT;
        };
    }

    private Token identifier() {
        int begin = pos;
        while (pos < s.length() && Character.isAlphabetic(s.charAt(pos))) pos++;
        String text = s.substring(begin, pos);
        return new Token(getType(text), text);
    }

    public Optional<Token> next() throws Error {
        while (pos < s.length()) {
            Character c = s.charAt(pos);
            switch (c) {
                case ' ' -> ++pos;
                case '(' -> {
                    ++pos;
                    return Optional.of(new Token(Token.Type.LPAREN, "("));
                }
                case ')' -> {
                    ++pos;
                    return Optional.of(new Token(Token.Type.RPAREN, ")"));
                }
                case '*' -> {
                    ++pos;
                    return Optional.of(new Token(Token.Type.STAR, "*"));
                }
                case Character first when Character.isAlphabetic(first) -> {
                    return Optional.of(identifier());
                }
                default -> throw new Error("scanner: unknown token: %c".formatted(c));
            }
        }
        return Optional.empty();
    }
}
