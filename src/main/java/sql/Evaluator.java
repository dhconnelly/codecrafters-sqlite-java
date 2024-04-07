package sql;

import storage.Database;
import storage.DatabaseException;
import storage.Row;
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

  private Value evaluate(AST.Expr expr, List<Row> rows)
  throws SQLException {
    return switch (expr) {
      case AST.FnCall(var fn, var ignored) when fn.equals("count") ->
          new Value.IntValue(rows.size());
      case AST.Expr ignored when rows.isEmpty() -> new Value.NullValue();
      default -> evaluate(expr, rows.getFirst());
    };
  }

  private Value evaluate(AST.Expr expr, Row row) throws SQLException {
    return switch (expr) {
      case AST.ColumnName(var name) -> row.get(name);
      case AST.StrLiteral(var s) -> new Value.StringValue(s);
      default -> throw new SQLException("invalid expr: %s".formatted(expr));
    };
  }

  private List<List<Value>> evaluate(List<AST.Expr> cols,
                                     List<Row> rows)
  throws SQLException {
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

  private boolean evaluate(AST.Cond filter, Row row)
  throws SQLException {
    return switch (filter) {
      case AST.Empty ignored -> true;
      case AST.Equal(var left, var right) -> {
        var leftVal = evaluate(left, row);
        var rightVal = evaluate(right, row);
        yield leftVal.equals(rightVal);
      }
    };
  }

  private List<Row> filter(AST.Cond filter, List<Row> rows)
  throws SQLException {
    List<Row> results = new ArrayList<>();
    for (var row : rows) {
      if (evaluate(filter, row)) results.add(row);
    }
    return results;
  }

  public void evaluate(AST.Statement statement)
  throws IOException, SQLException, DatabaseException {
    switch (statement) {
      case AST.CreateTableStatement ignored ->
          throw new SQLException("table creation not supported");
      case AST.SelectStatement(var cols, var cond, var table) -> {
        var t = db.getTable(table).orElseThrow(
            () -> new SQLException("no such table: %s".formatted(table)));
        var results = evaluate(cols, filter(cond, t.rows()));
        for (var row : results) {
          System.out.println(
              String.join("|", row.stream().map(Value::display).toList()));
        }
      }
    }
  }

  public void evaluate(String statement)
  throws SQLException, IOException, DatabaseException {
    evaluate(new Parser(new Scanner(statement)).statement());
  }
}
