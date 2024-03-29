package sql;

import java.util.Optional;

public class Scanner {
  private final String s;
  private int pos;
  private Optional<Token> peek;

  public Scanner(String s) {
    this.s = s;
    this.pos = 0;
    this.peek = Optional.empty();
  }

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }
  }

  private Token.Type getType(String text) {
    var type = switch (text.toLowerCase()) {
      case "select" -> Token.Type.SELECT;
      case "from" -> Token.Type.FROM;
      case "table" -> Token.Type.TABLE;
      case "create" -> Token.Type.CREATE;
      default -> Token.Type.IDENT;
    };
    return type;
  }

  private static boolean isIdentifier(char c) {
    return Character.isAlphabetic(c) || c == '_';
  }

  private Token identifier() {
    int begin = pos;
    while (pos < s.length() && isIdentifier(s.charAt(pos))) pos++;
    String text = s.substring(begin, pos);
    return new Token(getType(text), text);
  }

  public Optional<Token> peek() throws Error {
    if (!peek.isPresent()) peek = next();
    return peek;
  }

  public Optional<Token> next() throws Error {
    if (peek.isPresent()) {
      var next = peek;
      peek = Optional.empty();
      return next;
    }
    while (pos < s.length()) {
      Character c = s.charAt(pos);
      switch (c) {
        case ' ', '\n', '\t' -> ++pos;
        case ',' -> {
          ++pos;
          return Optional.of(new Token(Token.Type.COMMA, ","));
        }
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
