package sql;

public record Token(Token.Type type, String text) {
  public enum Type {
    SELECT,
    FROM,
    IDENT,
    LPAREN,
    RPAREN,
    STAR,
    CREATE,
    TABLE,
    INDEX,
    COMMA,
    WHERE,
    ON,
    EQ,
    STR,
  }
}
