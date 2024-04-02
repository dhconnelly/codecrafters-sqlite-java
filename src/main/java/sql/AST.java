package sql;

import java.util.List;

public class AST {
  public sealed interface Literal extends Expr permits StrLiteral {}
  public sealed interface Expr permits Star, FnCall, ColumnName, Literal {}
  public sealed interface Statement permits SelectStatement,
      CreateTableStatement {}
  public sealed interface Cond permits Equal, Empty {}
  public record StrLiteral(String s) implements Literal {}
  public record ColumnDefinition(String name, List<String> modifiers) {}
  public record CreateTableStatement(String name,
                                     List<ColumnDefinition> columns) implements Statement {}
  public record ColumnName(String name) implements Expr {}
  public record FnCall(String function, List<Expr> args) implements Expr {}
  public record Star() implements Expr {}
  public record SelectStatement(List<Expr> resultColumns, Cond filter,
                                String table) implements Statement {}
  public record Equal(Expr left, Expr right) implements Cond {}
  public record Empty() implements Cond {}
}
