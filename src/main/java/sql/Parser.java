package sql;

import java.util.ArrayList;
import java.util.List;

public class Parser {
  private final Scanner scanner;

  public Parser(Scanner scanner) {
    this.scanner = scanner;
  }

  private Token eat(Token.Type type) throws Scanner.Error, Error {
    Token tok = scanner.next().orElseThrow(Error::unexpectedEof);
    if (tok.type() != type) {
      throw new Error(
          "parser: want token %s, got %s".formatted(type, tok.type()));
    }
    return tok;
  }

  private boolean peekIs(Token.Type type) throws Scanner.Error {
    return scanner.peek().stream().allMatch(tok -> tok.type() == type);
  }

  private AST.Expr expr() throws Scanner.Error, Error {
    Token tok = scanner.next().orElseThrow(Error::unexpectedEof);
    switch (tok.type()) {
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
      default -> throw new Error(
          "parser: invalid expression: %s".formatted(tok.type()));
    }
  }

  public AST.SelectStatement select() throws Scanner.Error, Error {
    eat(Token.Type.SELECT);
    List<AST.Expr> columns = new ArrayList<>();
    while (!peekIs(Token.Type.FROM)) {
      columns.add(expr());
      if (!peekIs(Token.Type.FROM)) eat(Token.Type.COMMA);
    }
    eat(Token.Type.FROM);
    var table = eat(Token.Type.IDENT);
    return new AST.SelectStatement(columns, table.text());
  }

  private AST.ColumnDefinition columnDefinition() throws Error, Scanner.Error {
    var name = eat(Token.Type.IDENT);
    var modifiers = new ArrayList<String>();
    while (!peekIs(Token.Type.COMMA) && !peekIs(Token.Type.RPAREN)) {
      modifiers.add(eat(Token.Type.IDENT).text());
    }
    return new AST.ColumnDefinition(name.text(), modifiers);
  }

  public AST.CreateTableStatement createTable() throws Error, Scanner.Error {
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
    return new AST.CreateTableStatement(name.text(), columns);
  }

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }

    public static Error unexpectedEof() {
      return new Error("parser: unexpected eof");
    }
  }
}
