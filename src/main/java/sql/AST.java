package sql;

import java.util.List;

public class AST {
    public sealed interface Expr extends ResultColumn permits Star, FnCall {}
    public record FnCall(String function, List<Expr> args) implements Expr {}
    public sealed interface ResultColumn permits Star, Expr {}
    public sealed interface Statement permits SelectStatement {}
    public record Star() implements ResultColumn, Expr {}
    public record SelectStatement(List<ResultColumn> resultColumns, String table)
            implements Statement {}
}
