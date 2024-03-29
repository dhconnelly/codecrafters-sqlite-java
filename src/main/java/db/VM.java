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
import java.util.List;
import java.util.Optional;

public class VM {
  private final Database db;

  public VM(Database db) {
    this.db = db;
  }

  private List<Value> evaluate(AST.Expr expr, List<Record> rows, Table t) throws Error {
    switch (expr) {
      case AST.FnCall(var fn, var args) when fn.equals("count") -> {
        // ignore the args, just count the rows
        return List.of(new Value.IntValue(rows.size()));
      }
      case AST.ColumnName(var name) -> {
        Optional<Integer> column = t.getIndexForColumn(name);
        if (!column.isPresent()) {
          throw new Error("invalid column: %s".formatted(name));
        }
        return rows.stream().map(row -> row.valueAt(column.get())).toList();
      }
      default -> throw new Error("invalid expr: %s".formatted(expr));
    }
  }

  private List<Value> evaluate(AST.ResultColumn col, List<Record> rows,
                               Table t) throws Error {
    switch (col) {
      case AST.Expr expr -> {
        return evaluate(expr, rows, t);
      }
    }
  }

  private List<List<Value>> evaluate(List<AST.ResultColumn> cols,
                                     List<Record> rows,
                                     Table t) throws Error {
    if (cols.size() != 1)
      throw new Error("error: select supports exactly one column");
    return evaluate(cols.getFirst(), rows, t)
        .stream().map(col -> List.of(col)).toList();
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
              String.join(" ", row.stream().map(Value::display).toList()));
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
