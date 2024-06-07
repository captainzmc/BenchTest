package com.zmc.qps;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBResultSet;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class DuckDBArrowReadWriteQPSTest {

  public static void main(String[] args) throws IOException {
    String url = "jdbc:duckdb:/tmp/test.duckdb";
    int numQueries = 10000;
    int numRows = 10; // Set the number of rows to insert

    try (Connection connection = DriverManager.getConnection(url)) {
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, value DOUBLE)");
      }

      // Prepare an Arrow memory table
      RootAllocator allocator = new RootAllocator();
      Schema schema = new Schema(Arrays.asList(
          Field.nullable("id", Types.MinorType.INT.getType()),
          Field.nullable("value", Types.MinorType.FLOAT8.getType())
      ));
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

      // Measure write QPS
      Instant start = Instant.now();
      for (int i = 0; i < numQueries; i++) {
        // Populate the Arrow memory table
        root.allocateNew();
        for (int j = 0; j < numRows; j++) {
          ((IntVector) root.getVector("id")).set(j, j);
          ((Float8Vector) root.getVector("value")).set(j, Math.random());
        }
        root.setRowCount(numRows);

        // Write the Arrow memory table to DuckDB
        String insertSql = "INSERT INTO test (id, value) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
          for (int j = 0; j < numRows; j++) {
            pstmt.setInt(1, ((IntVector) root.getVector("id")).get(j));
            pstmt.setDouble(2, ((Float8Vector) root.getVector("value")).get(j));
            pstmt.addBatch();
          }
          pstmt.executeBatch();
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double writeQps = (double) numQueries * numRows / duration.toMillis() * 1000;
      System.out.println("Write QPS: " + writeQps);

      // Measure read QPS
      start = Instant.now();

      int count = 0;
      try (Connection conn = DriverManager.getConnection(url);
           PreparedStatement stmt = conn.prepareStatement("SELECT * from test");
           DuckDBResultSet resultset = (DuckDBResultSet) stmt.executeQuery()) {
        try (ArrowReader reader = (ArrowReader) resultset.arrowExportStream(allocator, 1024)) {
          while (reader.loadNextBatch()) {
            int currentRowCount = reader.getVectorSchemaRoot().getRowCount();
            count += currentRowCount;
          }
        }
      }

      end = Instant.now();
      duration = Duration.between(start, end);
      double readQps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Read QPS: " + readQps + ", Total Data: " + count);

      // Clean up
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE test");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}