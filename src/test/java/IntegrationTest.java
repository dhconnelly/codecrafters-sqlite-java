import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import query.QueryEngine;
import query.Row;
import query.Value;
import sql.SQLException;
import storage.StorageEngine;
import storage.StorageException;
import storage.Table;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IntegrationTest {
  private SeekableByteChannel file;

  @BeforeEach
  void setUp() throws IOException {
    var resource = Objects.requireNonNull(
        IntegrationTest.class.getResource("test.db"));
    file = Files.newByteChannel(Path.of(resource.getFile()));
  }

  @AfterEach
  void tearDown() throws IOException {
    file.close();
    file = null;
  }

  private List<Row> evaluate(String sql)
  throws SQLException, IOException, StorageException {
    var storage = new StorageEngine(file);
    return new QueryEngine(storage).evaluate(sql);
  }

  @Test
  void testDbinfo() throws IOException, StorageException, SQLException {
    var storage = new StorageEngine(file);
    var info = storage.getInfo();
    assertEquals(4096, info.get("database page size"));
    assertEquals(2, info.get("number of tables"));
  }

  @Test
  void testTables() throws SQLException, IOException, StorageException {
    var storage = new StorageEngine(file);
    assertEquals(
        storage.getTables().stream().map(Table::name)
               .collect(Collectors.toSet()),
        Set.of("companies", "sqlite_sequence"));
  }

  @Test
  void testCount() throws SQLException, IOException, StorageException {
    var rows = evaluate("SELECT count(*) FROM companies");
    assertEquals(rows, List.of(new Row(List.of(new Value.IntValue(55991)))));
  }

  @Test
  void testSelect() throws SQLException, IOException, StorageException {
    var names = evaluate(
        "SELECT name " +
        "FROM companies " +
        "WHERE locality = 'london, greater london, united kingdom'")
        .stream().map(row -> row.columns().getFirst().getString())
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
  void testIndex() throws SQLException, IOException, StorageException {
    var rows = new HashSet<>(evaluate(
        "SELECT id, name " +
        "FROM companies " +
        "WHERE country = 'republic of the congo'"));
    assertEquals(rows, Set.of(
        new Row(List.of(new Value.IntValue(517263),
                        new Value.StringValue("somedia"))),
        new Row(List.of(new Value.IntValue(509721),
                        new Value.StringValue("skytic telecom"))),
        new Row(List.of(new Value.IntValue(2995059),
                        new Value.StringValue(
                            "petroleum trading congo e&p sa"))),
        new Row(List.of(new Value.IntValue(2543747),
                        new Value.StringValue("its congo")))
    ));
  }
}
