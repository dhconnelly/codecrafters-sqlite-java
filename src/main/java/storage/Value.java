package storage;

import sql.AST;

public sealed interface Value {
  record NullValue() implements Value {}
  record IntValue(long value) implements Value {}
  record BlobValue(byte[] blob) implements Value {}
  record StringValue(String data) implements Value {}

  default String getString() {return ((StringValue) this).data;}
  default long getInt() {return ((IntValue) this).value;}

  static Value of(AST.Literal literal) {
    if (literal instanceof AST.StrLiteral s) return new StringValue(s.s());
    throw new IllegalArgumentException("unimplemented");
  }

  default String display() {
    return switch (this) {
      case IntValue(var x) -> "%d".formatted(x);
      case StringValue(var x) -> "%s".formatted(x);
      case NullValue() -> "NULL";
      case BlobValue(var ignored) -> "[blob]";
    };
  }
}
