package db;

import sql.AST;
import sql.Value;
import storage.Database;
import storage.Page;
import storage.Record;
import storage.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class VM {
  private final Database db;

  public VM(Database db) {
    this.db = db;
  }

  private static boolean isAggregation(AST.Expr expr) {
    return expr instanceof AST.FnCall;
  }

  private Value evaluate(AST.Expr expr, List<Record> rows, Table t) throws Error {
    return switch (expr) {
      case AST.FnCall(var fn, var ignored) when fn.equals("count") ->
          new Value.IntValue(rows.size());
      case AST.Expr ignored when rows.isEmpty() -> new Value.NullValue();
      default -> evaluate(expr, rows.getFirst(), t);
    };
  }

  private Value evaluate(AST.Expr expr, Record row, Table t) throws Error {
    switch (expr) {
      case AST.ColumnName(var name) -> {
        Optional<Integer> column = t.getIndexForColumn(name);
        if (column.isEmpty()) {
          throw new Error("invalid column: %s".formatted(name));
        }
        return row.get(column.get());
      }
      case AST.StrLiteral(var s) -> {
        return new Value.StringValue(s);
      }
      default -> throw new Error("invalid expr: %s".formatted(expr));
    }
  }

  private List<List<Value>> evaluate(List<AST.Expr> cols, List<Record> rows,
                                     Table t) throws Error {
    List<List<Value>> results = new ArrayList<>();
    if (cols.stream().anyMatch(VM::isAggregation)) {
      // TODO: figure out how to make streams work with exceptions
      List<Value> result = new ArrayList<>();
      for (var col : cols) result.add(evaluate(col, rows, t));
      results.add(result);
    } else {
      for (var row : rows) {
        List<Value> result = new ArrayList<>();
        for (var col : cols) result.add(evaluate(col, row, t));
        results.add(result);
      }
    }
    return results;
  }

  private boolean evaluate(AST.Cond filter, Record row, Table t) throws Error {
    return switch (filter) {
      case AST.Empty ignored -> true;
      case AST.Equal(var left, var right) -> {
        var leftVal = evaluate(left, row, t);
        var rightVal = evaluate(right, row, t);
        yield leftVal.equals(rightVal);
      }
    };
  }

  private List<Record> filter(AST.Cond filter, List<Record> rows, Table t) throws Error {
    List<Record> results = new ArrayList<>();
    for (var row : rows) {
      if (evaluate(filter, row, t)) results.add(row);
    }
    return results;
  }

  public void evaluate(AST.Statement statement) throws IOException,
                                                       Database.FormatException, Page.FormatException, Record.FormatException, Error {
    switch (statement) {
      case AST.CreateTableStatement ignored ->
          throw new Error("table creation not supported");
      case AST.SelectStatement(var cols, var cond, var table) -> {
        var t = db.getTable(table).orElseThrow(
            () -> new Error("no such table: %s".formatted(table)));
        var results = evaluate(cols, filter(cond, t.rows(), t), t);
        for (var row : results) {
          System.out.println(
              String.join("|", row.stream().map(Value::display).toList()));
        }
      }
    }
  }

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }
  }
}
