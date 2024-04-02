package storage;

import sql.AST;
import sql.Parser;
import sql.Scanner;

import java.util.List;

public class Table {
  private final String name;
  private final String type;
  private final Page page;
  private final String schema;
  private final AST.CreateTableStatement definition;

  public Table(String name, String type, Page page, String schema) throws Parser.Error, Scanner.Error {
    this.name = name;
    this.type = type;
    this.page = page;
    this.schema = schema;
    this.definition = new Parser(new Scanner(schema)).createTable();
  }

  public String name() {return name;}

  public String type() {return type;}

  public String schema() {return schema;}

  public List<Record> rows() throws Record.FormatException {
    return page.records().stream()
               .map(values -> Record.of(definition, values)).toList();
  }
}
