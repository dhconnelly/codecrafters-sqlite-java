package sql;

import java.util.Optional;

public sealed interface Value {
  record NullValue() implements Value {}
  record IntValue(int value) implements Value {}
  record BlobValue(byte[] blob) implements Value {}
  record StringValue(String data) implements Value {}

  default Optional<String> asString() {
    if (this instanceof StringValue(var data)) {
      return Optional.of(data);
    } else {
      return Optional.empty();
    }
  }

  default String display() {
    return switch (this) {
      case IntValue(var x) -> "%d".formatted(x);
      case StringValue(String x) -> "%s".formatted(x);
      default -> throw new AssertionError("unsupported: %s".formatted(this));
    };
  }
}
