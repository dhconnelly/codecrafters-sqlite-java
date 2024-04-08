package storage;

public record IndexedPage(Endpoint left, Endpoint right, int pageNumber) {
  public sealed interface Endpoint permits Unbounded, Bounded {}
  public record Unbounded() implements Endpoint {}
  public record Bounded(long endpoint) implements Endpoint {}
}
