package com.zmc.qps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public class DuckDBParquetReadWriteQPSTest {

  public static void main(String[] args) {
    String url = "jdbc:duckdb:/tmp/test.duckdb";
    int numQueries = 100000;
    String parquetFile = "/tmp/test.parquet";


    try (Connection connection = DriverManager.getConnection(url)) {
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value DOUBLE)");
      }

      // Measure write QPS
      Instant start = Instant.now();
      try (Statement stmt = connection.createStatement()) {
        for (int i = 0; i < numQueries; i++) {
          stmt.execute("INSERT INTO test VALUES (" + i + ", " + Math.random() + ")");
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double writeQps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Write QPS: " + writeQps);

      // Export the table to a Parquet file
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("COPY test TO '" + parquetFile + "' (FORMAT 'parquet')");
      }

      // Create a new table to read from the Parquet file
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE test_parquet AS SELECT * FROM parquet_scan('" + parquetFile + "')");
      }

      // Measure read QPS
      start = Instant.now();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test_parquet ORDER BY id")) {
        while (rs.next()) {
          int id = rs.getInt("id");
          double value = rs.getDouble("value");
        }
      }
      end = Instant.now();
      duration = Duration.between(start, end);
      double readQps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Scan QPS: " + readQps);


      // Measure QPS
      Instant start2 = Instant.now();
      try (Statement stmt = connection.createStatement()) {
        for (int i = 0; i < numQueries; i++) {
          try (ResultSet rs = stmt.executeQuery("SELECT * FROM test_parquet WHERE id = " + i)) {
            while (rs.next()) {
              int id = rs.getInt(1);
              double value = rs.getDouble(2);
            }
          }
        }
      }
      Instant end2 = Instant.now();
      Duration duration2 = Duration.between(start2, end2);
      double qps = (double) numQueries / duration2.toMillis() * 1000;
      System.out.println("Read QPS: " + qps);

      // Clean up

      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE test");
        stmt.execute("DROP TABLE test_parquet");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
