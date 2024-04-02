package sql;

import storage.Database;
import storage.Page;
import storage.Record;
import storage.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Evaluator {
  private final Database db;

  public Evaluator(Database db) {
    this.db = db;
  }

  private static boolean isAggregation(AST.Expr expr) {
    return expr instanceof AST.FnCall;
  }

  private Value evaluate(AST.Expr expr, List<Record> rows) throws Error {
    return switch (expr) {
      case AST.FnCall(var fn, var ignored) when fn.equals("count") ->
          new Value.IntValue(rows.size());
      case AST.Expr ignored when rows.isEmpty() -> new Value.NullValue();
      default -> evaluate(expr, rows.getFirst());
    };
  }

  private Value evaluate(AST.Expr expr, Record row) throws Error {
    switch (expr) {
      case AST.ColumnName(var name) -> {
        return row.get(name);
      }
      case AST.StrLiteral(var s) -> {
        return new Value.StringValue(s);
      }
      default -> throw new Error("invalid expr: %s".formatted(expr));
    }
  }

  private List<List<Value>> evaluate(List<AST.Expr> cols, List<Record> rows) throws Error {
    List<List<Value>> results = new ArrayList<>();
    if (cols.stream().anyMatch(Evaluator::isAggregation)) {
      // TODO: figure out how to make streams work with exceptions
      List<Value> result = new ArrayList<>();
      for (var col : cols) result.add(evaluate(col, rows));
      results.add(result);
    } else {
      for (var row : rows) {
        List<Value> result = new ArrayList<>();
        for (var col : cols) result.add(evaluate(col, row));
        results.add(result);
      }
    }
    return results;
  }

  private boolean evaluate(AST.Cond filter, Record row) throws Error {
    return switch (filter) {
      case AST.Empty ignored -> true;
      case AST.Equal(var left, var right) -> {
        var leftVal = evaluate(left, row);
        var rightVal = evaluate(right, row);
        yield leftVal.equals(rightVal);
      }
    };
  }

  private List<Record> filter(AST.Cond filter, List<Record> rows) throws Error {
    List<Record> results = new ArrayList<>();
    for (var row : rows) {
      if (evaluate(filter, row)) results.add(row);
    }
    return results;
  }

  public void evaluate(AST.Statement statement) throws IOException,
                                                       Database.FormatException
      , Page.FormatException, Record.FormatException, Error, Parser.Error,
                                                       Scanner.Error {
    switch (statement) {
      case AST.CreateTableStatement ignored ->
          throw new Error("table creation not supported");
      case AST.SelectStatement(var cols, var cond, var table) -> {
        var t = db.getTable(table).orElseThrow(
            () -> new Error("no such table: %s".formatted(table)));
        var results = evaluate(cols, filter(cond, t.rows()));
        for (var row : results) {
          System.out.println(
              String.join("|", row.stream().map(Value::display).toList()));
        }
      }
    }
  }

  public void evaluate(String statement) throws Scanner.Error, Parser.Error,
                                                Error, IOException,
                                                Database.FormatException,
                                                Page.FormatException,
                                                Record.FormatException {
    evaluate(new Parser(new Scanner(statement)).statement());
  }

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }
  }
}
