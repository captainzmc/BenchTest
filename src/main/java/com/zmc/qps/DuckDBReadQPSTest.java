package com.zmc.qps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public class DuckDBReadQPSTest {

  public static void main(String[] args) {
    String path = "/tmp/test.duckdb";
    int numQueries = 1000000;
    try {
      path = args[0];
      numQueries = Integer.parseInt(args[1]);
    } catch (ArrayIndexOutOfBoundsException e) {
    }
    String url = "jdbc:duckdb:" + path;


    try (Connection connection = DriverManager.getConnection(url)) {
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, value DOUBLE)");
      }

      // Insert some data
      try (Statement stmt = connection.createStatement()) {
        for (int i = 0; i < numQueries; i++) {
          stmt.execute("INSERT INTO test VALUES (" + i + ", " + Math.random() + ")");
        }
      }

      // Measure QPS
      Instant start = Instant.now();
      try (Statement stmt = connection.createStatement()) {
        for (int i = 0; i < numQueries; i++) {
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM test WHERE id = " + i)) {
            while (rs.next()) {
              int id = rs.getInt(1);
              double value = rs.getDouble(2);
            }
          }
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double qps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("QPS: " + qps);

      // Clean up
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE test");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
