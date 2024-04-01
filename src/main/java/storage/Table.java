package storage;

import sql.AST;
import sql.Parser;
import sql.Scanner;

import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

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

  public OptionalInt getIndexForColumn(String columnName) {
    var cols = definition.columns();
    return IntStream.range(0, cols.size())
                    .filter(i -> cols.get(i).name().equals(columnName))
                    .findFirst();
  }

  public String name() {return name;}

  public String type() {return type;}

  public String schema() {return schema;}

  public List<Record> rows() throws Record.FormatException {
    return page.readRecords();
  }
}
