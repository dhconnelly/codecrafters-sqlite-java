package storage;

import java.util.Map;

public record Row(int rowId, Map<String, Value> values) {
  public Value get(String column) {return values.get(column);}
}
