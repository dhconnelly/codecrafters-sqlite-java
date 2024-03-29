package db;

import sql.AST;
import sql.Parser;
import sql.Scanner;
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

  private Optional<Value> aggregate(AST.Expr expr, List<Record> rows) {
    return switch (expr) {
      case AST.FnCall(var fn, var ignored) when fn.equals("count") ->
          Optional.of(new Value.IntValue(rows.size()));
      default -> Optional.empty();
    };
  }

  private List<ResultColumn> aggregate(List<AST.Expr> columns,
                                       List<Record> rows) {
    return columns.stream().map(
        expr -> aggregate(expr, rows).map(this::resultOf)
                                     .orElse(resultOf(expr))).toList();
  }

  private Value evaluate(AST.Expr expr, Record row, Table t) throws Error {
    switch (expr) {
      case AST.ColumnName(var name) -> {
        Optional<Integer> column = t.getIndexForColumn(name);
        if (!column.isPresent()) {
          throw new Error("invalid column: %s".formatted(name));
        }
        return row.valueAt(column.get());
      }
      default -> throw new Error("invalid expr: %s".formatted(expr));
    }
  }

  private Value evaluate(ResultColumn col, Record row, Table t) throws Error {
    return switch (col) {
      case ExprColumn(var expr) -> evaluate(expr, row, t);
      case ValueColumn(var val) -> val;
    };
  }

  private List<List<Value>> evaluate(List<AST.Expr> cols, List<Record> rows,
                                     Table t) throws Error {
    var aggregated = aggregate(cols, rows);
    var results = new ArrayList<List<Value>>();
    for (var row : rows) {
      System.out.printf("evaluating row: %s\n", row);
      List<Value> result = new ArrayList<>();
      for (var col : aggregated) {
        System.out.printf("evaluating col: %s\n", col);
        result.add(evaluate(col, row, t));
      }
      results.add(result);
    }
    return results;
  }

  public void evaluate(AST.Statement statement) throws IOException,
                                                       Database.FormatException, Page.FormatException, Record.FormatException, Error, Parser.Error, Scanner.Error {
    switch (statement) {
      case AST.CreateTableStatement ignored -> {
        throw new Error("table creation not supported");
      }
      case AST.SelectStatement(var cols, var table) -> {
        var t = db.getTable(table).orElseThrow(
            () -> new Error("no such table: %s".formatted(table)));
        var rows = t.rows();
        var results = evaluate(cols, rows, t);
        for (var row : results) {
          System.out.println(
              String.join("|", row.stream().map(Value::display).toList()));
        }
      }
    }
  }

  private sealed interface ResultColumn permits ExprColumn, ValueColumn {}
  private record ExprColumn(AST.Expr expr) implements ResultColumn {}
  private record ValueColumn(Value val) implements ResultColumn {}

  private ResultColumn resultOf(AST.Expr expr) {return new ExprColumn(expr);}

  private ResultColumn resultOf(Value val) {return new ValueColumn(val);}

  public static class Error extends Exception {
    public Error(String message) {
      super(message);
    }
  }
}
