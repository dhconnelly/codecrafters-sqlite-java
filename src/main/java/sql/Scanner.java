package sql;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

public class Scanner {
  private final String s;
  private int pos;
  private final Queue<Token> lookahead;

  public Scanner(String s) {
    this.s = s;
    this.pos = 0;
    this.lookahead = new ArrayDeque<>();
  }

  private static boolean isIdentifier(char c) {
    return Character.isAlphabetic(c) || c == '_';
  }

  private static Token.Type getType(String text) {
    return switch (text.toLowerCase()) {
      case "select" -> Token.Type.SELECT;
      case "from" -> Token.Type.FROM;
      case "table" -> Token.Type.TABLE;
      case "create" -> Token.Type.CREATE;
      case "where" -> Token.Type.WHERE;
      default -> Token.Type.IDENT;
    };
  }

  private static Token.Type getType(char c) throws Error {
    return switch (c) {
      case ',' -> Token.Type.COMMA;
      case '=' -> Token.Type.EQ;
      case '(' -> Token.Type.LPAREN;
      case ')' -> Token.Type.RPAREN;
      case '*' -> Token.Type.STAR;
      default -> throw new Error("invalid token: %c".formatted(c));
    };
  }

  public Optional<Token> peek() throws Error {
    if (lookahead.isEmpty()) next().ifPresent(lookahead::add);
    return lookahead.stream().findFirst();
  }

  private String eat(char want) throws Error {
    char got = s.charAt(pos);
    if (got != want) throw new Error("want %c, got %c".formatted(want, got));
    ++pos;
    return String.valueOf(want);
  }

  private Token identifier() {
    int begin = pos;
    while (pos < s.length() && isIdentifier(s.charAt(pos))) pos++;
    String text = s.substring(begin, pos);
    return new Token(getType(text), text);
  }

  private Token text() throws Error {
    eat('\'');
    int begin = pos;
    while (pos < s.length() && s.charAt(pos) != '\'') ++pos;
    String text = s.substring(begin, pos);
    eat('\'');
    return new Token(Token.Type.STR, text);
  }

  public Optional<Token> next() throws Error {
    if (!lookahead.isEmpty()) return Optional.of(lookahead.poll());
    while (pos < s.length()) {
      char c = s.charAt(pos);
      switch (c) {
        case ' ', '\n', '\t' -> ++pos;
        case '\'' -> {return Optional.of(text());}
        case '=', ',', '(', ')', '*' -> {
          return Optional.of(new Token(getType(c), eat(c)));
        }
        default -> {
          if (isIdentifier(c)) return Optional.of(identifier());
          else throw new Error("unknown token: %c".formatted(c));
        }
      }
    }
    return Optional.empty();
  }

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }
  }
}
