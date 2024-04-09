import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sql.Evaluator;
import sql.SQLException;
import storage.Database;
import storage.DatabaseException;
import storage.Table;
import storage.Value;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IntegrationTest {
  private Database db;

  private static Database loadDb() throws IOException, DatabaseException {
    var resource = Objects.requireNonNull(
        IntegrationTest.class.getResource("test.db"));
    var channel = Files.newByteChannel(Path.of(resource.getFile()));
    return new Database(channel);
  }

  @BeforeEach
  void setUp() throws IOException, DatabaseException {
    db = loadDb();
  }

  @AfterEach
  void tearDown() throws IOException {
    db.close();
  }

  private List<List<Value>> evaluate(String sql)
  throws SQLException, IOException, DatabaseException {
    return new Evaluator(db).evaluate(sql);
  }

  @Test
  void testDbinfo() throws IOException, DatabaseException, SQLException {
    assertEquals(db.pageSize(), 4096);
    assertEquals(db.pageCount(), 1910);
    assertEquals(db.tables().size(), 2);
  }

  @Test
  void testTables() throws SQLException, IOException, DatabaseException {
    assertEquals(
        db.tables().stream().map(Table::name).collect(Collectors.toSet()),
        Set.of("companies", "sqlite_sequence"));
  }

  @Test
  void testCount() throws SQLException, IOException, DatabaseException {
    var rows = evaluate("SELECT count(*) FROM companies");
    assertEquals(rows, List.of(List.of(new Value.IntValue(55991))));
  }

  @Test
  void testSelect() throws SQLException, IOException, DatabaseException {
    var names = evaluate(
        "SELECT name " +
        "FROM companies " +
        "WHERE locality = 'london, greater london, united kingdom'")
        .stream().map(row -> row.getFirst().getString())
        .collect(Collectors.toSet());
    var expected = Set.of(
        "ascot barclay cyber security group",
        "align17",
        "intercash",
        "stonefarm capital llp",
        "blythe financial limited",
        "arnold wiggins & sons limited",
        "snowstream capital management ltd",
        "holmes&co | property",
        "tp international",
        "quantemplate",
        "clarity (previously hcl clarity)",
        "castille capital",
        "clayvard limited",
        "midnight tea studio",
        "tyntec",
        "trafalgar global",
        "transfertravel.com",
        "reign élan ltd");
    assertEquals(names, expected);
  }

  @Test
  void testIndex() throws SQLException, IOException, DatabaseException {
    var rows = new HashSet<>(evaluate(
        "SELECT id, name " +
        "FROM companies " +
        "WHERE country = 'republic of the congo'"));
    assertEquals(rows, Set.of(
        List.of(new Value.IntValue(517263), new Value.StringValue("somedia")),
        List.of(new Value.IntValue(509721),
                new Value.StringValue("skytic telecom")),
        List.of(new Value.IntValue(2995059),
                new Value.StringValue("petroleum trading congo e&p sa")),
        List.of(new Value.IntValue(2543747),
                new Value.StringValue("its congo"))
    ));
  }
}
