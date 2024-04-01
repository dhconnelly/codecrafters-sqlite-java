package sql;

import java.util.Optional;
import java.util.OptionalInt;

public sealed interface Value {
  record NullValue() implements Value {}
  record IntValue(int value) implements Value {}
  record BlobValue(byte[] blob) implements Value {}
  record StringValue(String data) implements Value {}

  default Optional<String> asString() {
    return this instanceof StringValue(var data)
        ? Optional.of(data)
        : Optional.empty();
  }

  default OptionalInt asInt() {
    return this instanceof IntValue(var x)
        ? OptionalInt.of(x)
        : OptionalInt.empty();
  }

  default String display() {
    return switch (this) {
      case IntValue(var x) -> "%d".formatted(x);
      case StringValue(String x) -> "%s".formatted(x);
      default -> throw new AssertionError("unsupported: %s".formatted(this));
    };
  }
}
