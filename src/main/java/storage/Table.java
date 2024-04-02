package storage;

import sql.AST;
import sql.Parser;
import sql.SQLException;
import sql.Scanner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {
  private final String name;
  private final String type;
  private final Page page;
  private final String schema;
  private final AST.CreateTableStatement definition;

  public Table(String name, String type, Page page, String schema)
  throws SQLException {
    this.name = name;
    this.type = type;
    this.page = page;
    this.schema = schema;
    this.definition = new Parser(new Scanner(schema)).createTable();
  }

  public String name() {return name;}
  public String type() {return type;}
  public String schema() {return schema;}

  private Record makeRecord(List<Value> values) {
    var record = new HashMap<String, Value>();
    for (int i = 0; i < definition.columns().size(); i++) {
      record.put(definition.columns().get(i).name(), values.get(i));
    }
    return new Record(record);
  }

  public List<Record> rows() throws DatabaseException {
    return page.records().stream().map(this::makeRecord).toList();
  }

  public record Record(Map<String, Value> values) {
    public Value get(String column) {return values.get(column);}
  }
}
