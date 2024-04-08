package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

public class Index {
  private final Database db;
  private final String name;
  private final Table table;
  private final Page<?> root;
  private final AST.CreateIndexStatement definition;

  public Index(Database db, String name, Table table, IndexPage<?> root,
               String schema) throws SQLException {
    this.db = db;
    this.name = name;
    this.table = table;
    this.root = root;
    this.definition = new Parser(new Scanner(schema)).createIndex();
  }

  public String name() {return name;}

  public Table table() {return table;}

  public AST.CreateIndexStatement definition() {return definition;}

  public Range find(String column, Value value) throws SQLException {
    if (!definition.column().equals(column)) {
      throw new SQLException(
          "index %s does not cover column %s".formatted(name, column));
    }
    throw new AssertionError("TODO");
  }

  public record Range(int beginRowId, int endRowId) {}
}
