package com.zmc.qps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public class DuckDBBatchWriteQPSTest {

  public static void main(String[] args) {
    String path = "/tmp/test.duckdb";
    int numQueries = 1000000;
    int batchSize = 1000000;
    try {
      path = args[0];
      numQueries = Integer.parseInt(args[1]);
      batchSize = Integer.parseInt(args[2]);
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    String url = "jdbc:duckdb:" + path;
//    String url = "jdbc:duckdb:";
    try (Connection connection = DriverManager.getConnection(url)) {
      connection.setAutoCommit(false);
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, value DOUBLE)");
      }

      // Measure write QPS
      Instant start = Instant.now();
      try (Statement stmt = connection.createStatement()) {
        for (int i = 0; i < numQueries; i++) {
          stmt.addBatch("INSERT INTO test VALUES (" + i + ", " + Math.random() + ")");
          if (i % batchSize == 0 || i == numQueries - 1) {
            stmt.executeBatch();
            connection.commit();
          }
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double qps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Write QPS: " + qps);

      // Clean up
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE test");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}