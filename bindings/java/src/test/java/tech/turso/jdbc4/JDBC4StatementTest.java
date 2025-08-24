package tech.turso.jdbc4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4StatementTest {

  private Statement stmt;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:turso:" + filePath;
    final JDBC4Connection connection = new JDBC4Connection(url, filePath, new Properties());
    stmt =
        connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Test
  void execute_ddl_should_return_false() throws Exception {
    assertFalse(stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);"));
  }

  @Test
  void execute_insert_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    assertFalse(stmt.execute("INSERT INTO users VALUES (1, 'turso');"));
  }

  @Test
  void execute_update_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso');");
    assertFalse(stmt.execute("UPDATE users SET username = 'seonwoo' WHERE id = 1;"));
  }

  @Test
  void execute_select_should_return_true() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso');");
    assertTrue(stmt.execute("SELECT * FROM users;"));
  }

  @Test
  void execute_select() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso 1')");
    stmt.execute("INSERT INTO users VALUES (2, 'turso 2')");
    stmt.execute("INSERT INTO users VALUES (3, 'turso 3')");

    ResultSet rs = stmt.executeQuery("SELECT * FROM users;");
    rs.next();
    int rowCount = 0;

    do {
      rowCount++;
      int id = rs.getInt(1);
      String username = rs.getString(2);

      assertEquals(id, rowCount);
      assertEquals(username, "turso " + rowCount);
    } while (rs.next());

    assertEquals(rowCount, 3);
    assertFalse(rs.next());
  }

  @Test
  void close_statement_test() throws Exception {
    stmt.close();
    assertTrue(stmt.isClosed());
  }

  @Test
  void double_close_is_no_op() throws SQLException {
    stmt.close();
    assertDoesNotThrow(() -> stmt.close());
  }

  @Test
  void operations_on_closed_statement_should_throw_exception() throws Exception {
    stmt.close();
    assertThrows(SQLException.class, () -> stmt.execute("SELECT 1;"));
  }

  @Test
  void execute_update_should_return_number_of_inserted_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (0);")).isEqualTo(1);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2);")).isEqualTo(2);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (3), (4), (5);")).isEqualTo(3);
  }

  @Test
  void execute_update_should_return_number_of_updated_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1 INT);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2), (3);")).isEqualTo(3);
    assertThat(stmt.executeUpdate("UPDATE s1 SET c1 = 0;")).isEqualTo(3);
  }

  @Test
  void execute_update_should_return_number_of_deleted_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2), (3);")).isEqualTo(3);

    assertThat(stmt.executeUpdate("DELETE FROM s1")).isEqualTo(3);
  }

  /** Tests for batch processing functionality */
  @Test
  void testAddBatch_single_statement() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add a single batch command
    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");

    // Execute batch
    int[] updateCounts = stmt.executeBatch();

    // Verify results
    assertThat(updateCounts).hasSize(1);
    assertThat(updateCounts[0]).isEqualTo(1);

    // Verify data was inserted
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(1);
  }

  @Test
  void testAddBatch_multiple_statements() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add multiple batch commands
    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    // Execute batch
    int[] updateCounts = stmt.executeBatch();

    // Verify results
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1);
    assertThat(updateCounts[1]).isEqualTo(1);
    assertThat(updateCounts[2]).isEqualTo(1);

    // Verify all data was inserted
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(3);
  }

  @Test
  void testAddBatch_with_updates_and_deletes() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Insert initial data
    stmt.execute(
        "INSERT INTO batch_test VALUES (1, 'initial1'), (2, 'initial2'), (3, 'initial3');");

    // Add batch commands with different operations
    stmt.addBatch("UPDATE batch_test SET value = 'updated';");
    stmt.addBatch("DELETE FROM batch_test WHERE id = 2;");
    stmt.addBatch("INSERT INTO batch_test VALUES (4, 'new');");

    // Execute batch
    int[] updateCounts = stmt.executeBatch();

    // Verify update counts
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(3); // UPDATE affected 3 row
    assertThat(updateCounts[1]).isEqualTo(1); // DELETE affected 1 row
    assertThat(updateCounts[2]).isEqualTo(1); // INSERT affected 1 row

    // Verify final state
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(3); // 3 initial - 1 deleted + 1 inserted = 3
  }

  @Test
  void testClearBatch() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add batch commands
    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");

    // Clear the batch
    stmt.clearBatch();

    // Execute batch should return empty array
    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();

    // Verify no data was inserted
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(0);
  }

  @Test
  void testBatch_with_DDL_statements() throws SQLException {
    // DDL statements should work in batch
    stmt.addBatch("CREATE TABLE batch_test1 (id INTEGER);");
    stmt.addBatch("CREATE TABLE batch_test2 (id INTEGER);");
    stmt.addBatch("CREATE TABLE batch_test3 (id INTEGER);");

    // Execute batch
    int[] updateCounts = stmt.executeBatch();

    // DDL statements typically return 0 for update count
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(0);
    assertThat(updateCounts[1]).isEqualTo(0);
    assertThat(updateCounts[2]).isEqualTo(0);

    // Verify tables were created by inserting data
    assertDoesNotThrow(() -> stmt.execute("INSERT INTO batch_test1 VALUES (1);"));
    assertDoesNotThrow(() -> stmt.execute("INSERT INTO batch_test2 VALUES (1);"));
    assertDoesNotThrow(() -> stmt.execute("INSERT INTO batch_test3 VALUES (1);"));
  }

  @Test
  void testBatch_with_SELECT_should_throw_exception() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");
    stmt.execute("INSERT INTO batch_test VALUES (1, 'test1');");

    // Add a SELECT statement to batch (not allowed)
    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");
    stmt.addBatch("SELECT * FROM batch_test;"); // This should cause an exception
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    // Execute batch should throw BatchUpdateException
    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    // Verify exception message
    assertTrue(exception.getMessage().contains("Batch commands cannot return result sets"));

    // Verify update counts for executed statements before the failure
    int[] updateCounts = exception.getUpdateCounts();
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1); // First INSERT succeeded
    assertThat(updateCounts[1]).isEqualTo(Statement.EXECUTE_FAILED); // SELECT failed
    // The third statement may not have been executed depending on implementation
  }

  @Test
  void testBatch_with_null_command_should_throw_exception() throws SQLException {
    // Adding null command should throw SQLException
    assertThrows(SQLException.class, () -> stmt.addBatch(null));
  }

  @Test
  void testBatch_operations_on_closed_statement_should_throw_exception() throws SQLException {
    stmt.close();

    // All batch operations should throw SQLException on closed statement
    assertThrows(SQLException.class, () -> stmt.addBatch("INSERT INTO test VALUES (1);"));
    assertThrows(SQLException.class, () -> stmt.clearBatch());
    assertThrows(SQLException.class, () -> stmt.executeBatch());
  }

  @Test
  void testBatch_with_syntax_error_should_throw_exception() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add batch commands with a syntax error
    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INVALID SQL SYNTAX;"); // This should cause an exception
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    // Execute batch should throw BatchUpdateException
    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    // Verify update counts show partial execution
    int[] updateCounts = exception.getUpdateCounts();
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1); // First INSERT succeeded
    assertThat(updateCounts[1]).isEqualTo(Statement.EXECUTE_FAILED); // Invalid SQL failed
  }

  @Test
  void testBatch_empty_batch_returns_empty_array() throws SQLException {
    // Execute empty batch
    int[] updateCounts = stmt.executeBatch();

    // Should return empty array
    assertThat(updateCounts).isEmpty();
  }

  @Test
  void testBatch_clears_after_successful_execution() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add and execute batch
    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.executeBatch();

    // Execute batch again should return empty array (batch was cleared)
    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();
  }

  @Test
  void testBatch_clears_after_failed_execution() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    // Add batch with SELECT statement that will fail
    stmt.addBatch("SELECT * FROM batch_test;");

    // Execute batch should fail
    assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    // Execute batch again should return empty array (batch was cleared after failure)
    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();
  }
}
