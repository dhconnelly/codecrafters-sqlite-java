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

  public Index(Database db, String name, Table table, Page<?> root,
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
}
