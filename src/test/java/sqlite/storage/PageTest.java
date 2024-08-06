package sqlite.storage;

import org.junit.jupiter.api.Test;
import sqlite.storage.Pointer.Bounded;
import sqlite.storage.Pointer.Unbounded;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class PageTest {
  // create the page at an offset to catch more bugs
  private static final int PAGE_SIZE = 4096;
  private static final int PAGE_OFFSET = 17;

  private static ByteBuffer testPage(
      Page.Type type,
      int numCells,
      OptionalInt interiorPageRightMostPointer,
      // int array to allow passing an array literal without suicide
      int[][] cells
  ) {
    final var buf = ByteBuffer
        .allocate(PAGE_SIZE + PAGE_OFFSET)
        .order(ByteOrder.BIG_ENDIAN);

    // fill the header
    buf.position(PAGE_OFFSET).put(type.value);
    buf.position(PAGE_OFFSET + 3).putShort((short) numCells);
    interiorPageRightMostPointer.ifPresent(pageNumber -> buf
        .position(PAGE_OFFSET + 8)
        .putInt(pageNumber)
    );

    // fill the pointer and content arrays
    final int afterHeader = PAGE_OFFSET + Page.headerSize(type);
    int pointerOffset = afterHeader;
    int cellOffset = afterHeader + 2 * cells.length;
    for (var cell : cells) {
      buf.position(pointerOffset).putShort((short) cellOffset);
      buf.position(cellOffset).put(toBytes(cell));
      pointerOffset += 2;
      cellOffset += cell.length;
    }

    return buf;
  }

  static byte[] toBytes(int[] ints) {
    byte[] bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) bytes[i] = (byte) ints[i];
    return bytes;
  }

  @Test
  public void testInvalidPage() {
    fail();
  }

  @Test
  public void testTableInteriorPage() {
    int[][] cells = new int[][]{
        // <page number, integer key>
        concat(new int[]{0, 0, 0, 1}, new int[]{2}),
        concat(new int[]{0, 0, 0, 2}, new int[]{4}),
        concat(new int[]{0, 0, 0, 3}, new int[]{6}),
        concat(new int[]{0, 0, 0, 4}, new int[]{8}),
    };
    var buf = testPage(
        Page.Type.TABLE_INTERIOR, cells.length, OptionalInt.of(5), cells);

    var page = Page.from(buf, PAGE_OFFSET, StandardCharsets.UTF_8);

    page.asTablePage();
    assertThrows(StorageException.class, page::asIndexPage);
    assertEquals(4, page.getNumCells());
    assertEquals(5, page.numRecords());
    assertEquals(StandardCharsets.UTF_8, page.getCharset());
    assertEquals(12, page.headerSize());
    assertEquals(
        List.of(
            new Pointer<>(new Unbounded<>(), new Bounded<>(2L), 1),
            new Pointer<>(new Bounded<>(2L), new Bounded<>(4L), 2),
            new Pointer<>(new Bounded<>(4L), new Bounded<>(6L), 3),
            new Pointer<>(new Bounded<>(6L), new Bounded<>(8L), 4),
            new Pointer<>(new Bounded<>(8L), new Unbounded<>(), 5)
        ),
        page.records().toList()
    );
  }

  int[] concat(int[] left, int[] right) {
    return IntStream.concat(Arrays.stream(left), Arrays.stream(right))
                    .toArray();
  }

  @Test
  public void testTableLeafPage() {
    fail();
  }

  @Test
  public void testIndexInteriorPage() {
    fail();
  }

  @Test
  public void testIndexLeafPage() {
    fail();
  }
}
