package sql;

import storage.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Evaluator {
  private final Database db;

  public Evaluator(Database db) {
    this.db = db;
  }

  private static boolean isAggregation(AST.Expr expr) {
    return expr instanceof AST.FnCall;
  }

  private Value evaluate(AST.Expr expr, List<Table.Row> rows)
  throws SQLException {
    return switch (expr) {
      case AST.FnCall(var fn, var ignored) when fn.equals("count") ->
          new Value.IntValue(rows.size());
      case AST.Expr ignored when rows.isEmpty() -> new Value.NullValue();
      default -> evaluate(expr, rows.getFirst());
    };
  }

  private Value evaluate(AST.Expr expr, Table.Row row) throws SQLException {
    return switch (expr) {
      case AST.ColumnName(var name) -> row.get(name);
      case AST.StrLiteral(var s) -> new Value.StringValue(s);
      default -> throw new SQLException("invalid expr: %s".formatted(expr));
    };
  }

  private List<List<Value>> evaluate(List<AST.Expr> cols,
                                     List<Table.Row> rows)
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

  private boolean evaluate(AST.Filter filter, Table.Row row)
  throws SQLException {
    return evaluate(filter.column(), row).equals(evaluate(filter.value(), row));
  }

  private Optional<Index> findIndexForFilter(AST.Filter f)
  throws SQLException, IOException, DatabaseException {
    // TODO: for multi-column indices we would want to consider column ordering
    return db.indices().stream()
             .filter(idx -> idx.definition().column().equals(f.column().name()))
             .findFirst();
  }

  // TODO: stream not rows
  private List<Table.Row> getRows(Table t, AST.Filter filter)
  throws SQLException, IOException, DatabaseException {
    Optional<Index> maybeIndex = findIndexForFilter(filter);
    if (maybeIndex.isPresent()) {
      var rowIds = maybeIndex.get().find(filter.column().name(),
                                         Value.of(filter.value()));
      List<Table.Row> results = new ArrayList<>();
      for (long rowId : rowIds) {
        results.add(t.get(rowId).orElseThrow(() -> new AssertionError(
            "row not found in table for indexed id %d".formatted(rowId))));
      }
      return results;
    } else {
      List<Table.Row> results = new ArrayList<>();
      for (var row : t.rows()) {
        if (evaluate(filter, row)) results.add(row);
      }
      return results;
    }
  }

  public List<List<Value>> evaluate(AST.Statement statement)
  throws IOException, SQLException, DatabaseException {
    switch (statement) {
      case AST.CreateTableStatement ignored ->
          throw new SQLException("table creation not supported");
      case AST.CreateIndexStatement ignored ->
          throw new SQLException("index creation not supported");
      case AST.SelectStatement(var cols, var cond, var table) -> {
        var t = db.getTable(table).orElseThrow(
            () -> new SQLException("no such table: %s".formatted(table)));
        var rows = cond.isPresent() ? getRows(t, cond.get()) : t.rows();
        return evaluate(cols, rows);
      }
    }
  }

  public List<List<Value>> evaluate(String statement)
  throws SQLException, IOException, DatabaseException {
    return evaluate(new Parser(new Scanner(statement)).statement());
  }
}
