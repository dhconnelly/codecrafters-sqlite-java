package storage;

import sql.AST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record Record(Map<String, Value> values) {
  public static Record of(AST.CreateTableStatement definition,
                          List<Value> record) {
    var values = new HashMap<String, Value>();
    for (int i = 0; i < definition.columns().size(); i++) {
      values.put(definition.columns().get(i).name(), record.get(i));
    }
    return new Record(values);
  }

  public Value get(String column) {return values.get(column);}

  public static final class FormatException extends Exception {
    public FormatException(String message) {
      super(message);
    }
  }
}
