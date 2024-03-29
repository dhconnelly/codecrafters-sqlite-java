package sql;

import java.util.List;

public class AST {
  public sealed interface Expr permits Star, FnCall, ColumnName {}
  public sealed interface Statement permits SelectStatement,
      CreateTableStatement {}
  public record ColumnDefinition(String name, List<String> modifiers) {}
  public record CreateTableStatement(String name,
                                     List<ColumnDefinition> columns) implements Statement {}
  public record ColumnName(String name) implements Expr {}
  public record FnCall(String function, List<Expr> args) implements Expr {}
  public record Star() implements Expr {}
  public record SelectStatement(List<Expr> resultColumns,
                                String table) implements Statement {}
}
