package sqlite.query;

import sqlite.sql.AST;
import sqlite.sql.Parser;
import sqlite.sql.SQLException;
import sqlite.sql.Scanner;
import sqlite.storage.Index;
import sqlite.storage.StorageEngine;
import sqlite.storage.StorageException;
import sqlite.storage.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class QueryEngine {
  private final StorageEngine db;

  public QueryEngine(StorageEngine db) {
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

  private List<Row> evaluate(List<AST.Expr> cols,
                             List<Table.Row> rows)
  throws SQLException {
    List<Row> results = new ArrayList<>();
    if (cols.stream().anyMatch(QueryEngine::isAggregation)) {
      // TODO: figure out how to make streams work with exceptions
      var result = new ArrayList<Value>();
      for (var col : cols) result.add(evaluate(col, rows));
      results.add(new Row(result));
    } else {
      for (var row : rows) {
        var result = new ArrayList<Value>();
        for (var col : cols) result.add(evaluate(col, row));
        results.add(new Row(result));
      }
    }
    return results;
  }

  private boolean evaluate(AST.Filter filter, Table.Row row)
  throws SQLException {
    return evaluate(filter.column(), row).equals(evaluate(filter.value(), row));
  }

  private Optional<Index> findIndexForFilter(AST.Filter f)
  throws SQLException, IOException, StorageException {
    // TODO: for multi-column indices we would want to consider column ordering
    return db.getIndices().stream()
             .filter(idx -> idx.definition().column().equals(f.column().name()))
             .findFirst();
  }

  private static Value valueOf(AST.Literal literal) {
    if (literal instanceof AST.StrLiteral s)
      return new Value.StringValue(s.s());
    throw new IllegalArgumentException("unimplemented");
  }

  private List<Table.Row> getRows(Table t, AST.Filter filter)
  throws SQLException, IOException, StorageException {
    Optional<Index> maybeIndex = findIndexForFilter(filter);
    if (maybeIndex.isPresent()) {
      var rowIds = maybeIndex.get()
                             .findMatchingRecordIds(filter.column().name(),
                                                    valueOf(filter.value()));
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

  private List<Row> evaluate(AST.Statement statement)
  throws IOException, SQLException, StorageException {
    switch (statement) {
      case AST.CreateTableStatement ignored ->
          throw new SQLException("table creation not supported");
      case AST.CreateIndexStatement ignored ->
          throw new SQLException("index creation not supported");
      case AST.SelectStatement(var cols, var cond, var tableName) -> {
        var table = db.getTables().stream()
                      .filter(t -> t.name().equals(tableName))
                      .findAny()
                      .orElseThrow(
                          () -> new SQLException(
                              "no such table: %s".formatted(tableName)));
        var rows = cond.isPresent() ? getRows(table, cond.get()) : table.rows();
        return evaluate(cols, rows);
      }
    }
  }

  // TODO: stream
  public List<Row> evaluate(String statement)
  throws SQLException, IOException, StorageException {
    return evaluate(new Parser(new Scanner(statement)).statement());
  }
}
