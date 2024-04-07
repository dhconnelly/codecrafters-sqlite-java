package sql;

import java.util.ArrayList;
import java.util.List;

public class Parser {
  private final Scanner scanner;

  public Parser(Scanner scanner) {
    this.scanner = scanner;
  }

  private Token advance() throws SQLException {
    return scanner.next().orElseThrow(
        () -> new SQLException("parser: unexpected eof"));
  }

  private Token eat(Token.Type type) throws SQLException {
    Token tok = advance();
    if (tok.type() != type) {
      throw new SQLException(
          "parser: want token %s, got %s".formatted(type, tok.type()));
    }
    return tok;
  }

  private boolean peekIs(Token.Type type) throws SQLException {
    return scanner.peek().stream().anyMatch(tok -> tok.type() == type);
  }

  private void eof() throws SQLException {
    var peeked = scanner.peek();
    if (peeked.isPresent()) {
      throw new SQLException(
          "parser: expected eof, got %s".formatted(peeked.get().type()));
    }
  }

  private AST.Expr expr() throws SQLException {
    Token tok = advance();
    switch (tok.type()) {
      case Token.Type.STR -> {
        return new AST.StrLiteral(tok.text());
      }
      case Token.Type.STAR -> {
        return new AST.Star();
      }
      case Token.Type.IDENT -> {
        if (peekIs(Token.Type.LPAREN)) {
          eat(Token.Type.LPAREN);
          var arg = this.expr();
          eat(Token.Type.RPAREN);
          return new AST.FnCall(tok.text().toLowerCase(), List.of(arg));
        } else {
          return new AST.ColumnName(tok.text());
        }
      }
      default -> throw new SQLException(
          "parser: invalid expression: %s".formatted(tok.type()));
    }
  }

  private AST.Cond cond() throws SQLException {
    if (!peekIs(Token.Type.WHERE)) return new AST.Empty();
    eat(Token.Type.WHERE);
    var left = expr();
    eat(Token.Type.EQ);
    var right = expr();
    return new AST.Equal(left, right);
  }

  public AST.SelectStatement select() throws SQLException {
    eat(Token.Type.SELECT);
    List<AST.Expr> columns = new ArrayList<>();
    while (!peekIs(Token.Type.FROM)) {
      columns.add(expr());
      if (!peekIs(Token.Type.FROM)) eat(Token.Type.COMMA);
    }
    eat(Token.Type.FROM);
    var table = eat(Token.Type.IDENT);
    var filter = cond();
    eof();
    return new AST.SelectStatement(columns, filter, table.text());
  }

  private AST.ColumnDefinition columnDefinition() throws SQLException {
    var name = eat(Token.Type.IDENT);
    var modifiers = new ArrayList<String>();
    while (!peekIs(Token.Type.COMMA) && !peekIs(Token.Type.RPAREN)) {
      modifiers.add(eat(Token.Type.IDENT).text());
    }
    return new AST.ColumnDefinition(name.text(), modifiers);
  }

  public AST.CreateTableStatement createTable() throws SQLException {
    eat(Token.Type.CREATE);
    eat(Token.Type.TABLE);
    var name = eat(Token.Type.IDENT);
    eat(Token.Type.LPAREN);
    var columns = new ArrayList<AST.ColumnDefinition>();
    while (!peekIs(Token.Type.RPAREN)) {
      columns.add(columnDefinition());
      if (!peekIs(Token.Type.RPAREN)) eat(Token.Type.COMMA);
    }
    eat(Token.Type.RPAREN);
    eof();
    return new AST.CreateTableStatement(name.text(), columns);
  }

  public AST.CreateIndexStatement createIndex() throws SQLException {
    eat(Token.Type.CREATE);
    eat(Token.Type.INDEX);
    var name = eat(Token.Type.IDENT);
    eat(Token.Type.ON);
    var table = eat(Token.Type.IDENT);
    eat(Token.Type.LPAREN);
    var columns = new ArrayList<String>();
    while (!peekIs(Token.Type.RPAREN)) {
      columns.add(eat(Token.Type.IDENT).text());
      if (!peekIs(Token.Type.RPAREN)) eat(Token.Type.COMMA);
    }
    eat(Token.Type.RPAREN);
    return new AST.CreateIndexStatement(name.text(), table.text(), columns);
  }

  public AST.Statement statement() throws SQLException {
    return peekIs(Token.Type.CREATE) ? createTable() : select();
  }
}
