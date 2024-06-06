package com.zmc.qps;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public class DuckDBAppendWriteQPSTest {

  public static void main(String[] args) {
    String path = "/tmp/test2.duckdb";
    String url = "jdbc:duckdb:";
    int numQueries = 1000000;

    try (Connection connection = DriverManager.getConnection(url+path)) {
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, value DOUBLE)");
      }

      // Measure write QPS
      Instant start = Instant.now();
      try (DuckDBAppender appender = ((DuckDBConnection) connection).createAppender("main", "test")) {
        for (int i = 0; i < numQueries; i++) {
          appender.beginRow();
          appender.append(i);
          appender.append(Math.random());
          appender.endRow();
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