package sqlite.sql;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import static sqlite.sql.Token.Type.IDENT;
import static sqlite.sql.Token.Type.STR;

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

  private static Optional<Token.Type> getKeyword(String name) {
    try {
      return Optional.of(Token.Type.valueOf(name.toUpperCase()));
    } catch (IllegalArgumentException ignored) {
      return Optional.empty();
    }
  }

  private static Token.Type getType(char c) {
    return switch (c) {
      case ',' -> Token.Type.COMMA;
      case '=' -> Token.Type.EQ;
      case '(' -> Token.Type.LPAREN;
      case ')' -> Token.Type.RPAREN;
      case '*' -> Token.Type.STAR;
      default -> throw new SQLException("scanner: bad token: %c".formatted(c));
    };
  }

  @Deprecated
  public Optional<Token> peek() {
    if (lookahead.isEmpty()) next().ifPresent(lookahead::add);
    return lookahead.stream().findFirst();
  }

  public boolean isEof() {
    return peek().isEmpty();
  }

  private String eat(char want) {
    if (pos >= s.length()) {
      throw new SQLException("scanner: unexpected eof");
    }
    char got = s.charAt(pos);
    if (got != want) {
      throw new SQLException("scanner: want %c, got %c".formatted(want, got));
    }
    ++pos;
    return String.valueOf(want);
  }

  private Token identifier() {
    int begin = pos;
    while (pos < s.length() && isIdentifier(s.charAt(pos))) pos++;
    var text = s.substring(begin, pos);
    return getKeyword(text).map(Token::of).orElse(Token.of(IDENT, text));
  }

  private String stringLiteral(char delim) {
    eat(delim);
    int begin = pos;
    while (pos < s.length() && s.charAt(pos) != delim) ++pos;
    var text = s.substring(begin, pos);
    eat(delim);
    return text;
  }

  public Token nextToken() {
    return next().get();
  }

  @Deprecated
  public Optional<Token> next() {
    if (!lookahead.isEmpty()) return Optional.of(lookahead.poll());
    while (pos < s.length()) {
      char c = s.charAt(pos);
      switch (c) {
        case ' ', '\n', '\t' -> ++pos;
        case '\'' -> {
          return Optional.of(Token.of(STR, stringLiteral(c)));
        }
        case '"' -> {
          return Optional.of(Token.of(IDENT, stringLiteral(c)));
        }
        case '=', ',', '(', ')', '*' -> {
          eat(c);
          return Optional.of(Token.of(getType(c)));
        }
        default -> {
          if (isIdentifier(c)) return Optional.of(identifier());
          else throw new SQLException("scanner: bad token: %c".formatted(c));
        }
      }
    }
    return Optional.empty();
  }
}
