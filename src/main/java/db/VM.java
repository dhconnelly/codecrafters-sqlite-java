package db;

import sql.AST;
import sql.Value;
import storage.Database;
import storage.Page;
import storage.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VM {
    private final Database db;

    public VM(Database db) {
        this.db = db;
    }

    public static class Error extends Exception {
        public Error(String message) {
            super(message);
        }
    }

    private List<Value> evaluate(AST.Expr expr, List<Record> rows) throws Error {
        switch (expr) {
            case AST.FnCall(var fn, var args) when fn.equals("count") -> {
                // ignore the args, just count the rows
                return List.of(new Value.IntValue(rows.size()));
            }
            default -> throw new Error("invalid expr: %s".formatted(expr));
        }
    }

    private List<Value> evaluate(AST.ResultColumn col, List<Record> rows) throws Error {
        switch (col) {
            case AST.Expr expr -> {
                return evaluate(expr, rows);
            }
        }
    }

    private List<List<Value>> evaluate(List<AST.ResultColumn> cols, List<Record> rows) throws Error {
        var results = new ArrayList<List<Value>>();
        for (AST.ResultColumn col : cols) results.add(evaluate(col, rows));
        return results;
    }

    public void evaluate(AST.Statement statement) throws IOException, Database.FormatException,
                                                         Page.FormatException,
                                                         Record.FormatException, Error {
        switch (statement) {
            case AST.SelectStatement(var cols, var table) -> {
                var t = db.getTable(table)
                          .orElseThrow(() -> new Error("no such table: %s".formatted(table)));
                var rows = t.rows();
                var results = evaluate(cols, rows);
                for (var row : results) {
                    for (var col : row) System.out.printf("%s ", Value.display(col));
                    System.out.println();
                }
            }
        }
    }
}
