package sql;

public sealed interface Value {
    record NullValue() implements Value {}
    record IntValue(int value) implements Value {}
    record FloatValue(double value) implements Value {}
    record BlobValue(byte[] blob) implements Value {}
    record StringValue(String data) implements Value {}

    static String display(Value v) {
        return switch (v) {
            case IntValue(var x) -> "%d".formatted(x);
            default -> throw new AssertionError("TODO");
        };
    }
}
