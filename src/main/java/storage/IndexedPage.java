package storage;

public record IndexedPage<Key>(Endpoint<Key> left, Endpoint<Key> right,
                               int pageNumber) {
  public sealed interface Endpoint<Key> permits Unbounded, Bounded {}
  public record Unbounded<Key>() implements Endpoint<Key> {}
  public record Bounded<Key>(Key endpoint) implements Endpoint<Key> {}
}
