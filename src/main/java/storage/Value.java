package storage;

public sealed interface Value {
  record NullValue() implements Value {}
  record IntValue(int value) implements Value {}
  record BlobValue(byte[] blob) implements Value {}
  record StringValue(String data) implements Value {}

  default String getString() {return ((StringValue) this).data;}
  default int getInt() {return ((IntValue) this).value;}

  default String display() {
    return switch (this) {
      case IntValue(var x) -> "%d".formatted(x);
      case StringValue(var x) -> "%s".formatted(x);
      case NullValue() -> "NULL";
      case BlobValue(var ignored) -> "[blob]";
    };
  }
}
