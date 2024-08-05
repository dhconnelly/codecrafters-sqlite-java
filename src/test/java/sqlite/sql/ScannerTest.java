package sqlite.sql;

import common.NonNull;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static sqlite.sql.Token.Type.*;

public class ScannerTest {
  private static List<Token> scanAll(@NonNull final String text) {
    var scanner = new Scanner(text);
    var tokens = new ArrayList<Token>();
    while (!scanner.isEof()) {
      tokens.add(scanner.nextToken());
    }
    return tokens;
  }

  @Test
  public void testScanWhitespace() {
    assertEquals(List.of(), scanAll("    \n   \t  "));
    assertEquals(List.of(Token.of(WHERE)), scanAll("   where  \n"));
  }

  @Test
  public void testScanErrors() {
    assertThrows(SQLException.class, () -> scanAll(" ^  "));
    assertThrows(SQLException.class, () -> scanAll(" 'foo  "));
    assertThrows(SQLException.class, () -> scanAll(" \"foo   "));
  }

  @Test
  public void testScan() {
    var expected = List.of(
        Token.of(SELECT),
        Token.of(FROM),
        Token.of(LPAREN),
        Token.of(RPAREN),
        Token.of(STAR),
        Token.of(CREATE),
        Token.of(TABLE),
        Token.of(INDEX),
        Token.of(COMMA),
        Token.of(WHERE),
        Token.of(ON),
        Token.of(EQ),
        Token.of(IDENT, "foo"),
        Token.of(IDENT, "bar baz"),
        Token.of(STR, "blah")
    );
    var actual = scanAll(
        "select from ()* create table index,where on= foo \"bar baz\" 'blah'"
    );
    assertEquals(expected, actual);
  }
}
