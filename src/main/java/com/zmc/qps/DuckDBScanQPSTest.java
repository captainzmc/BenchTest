package com.zmc.qps;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

public class DuckDBScanQPSTest {

  public static void main(String[] args) {
    String path = "/tmp/test2.duckdb";
    String url = "jdbc:duckdb:";
    int numQueries = 1000000;

    try (Connection connection = DriverManager.getConnection(url+path)) {
      // Create a test table
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, value DOUBLE)");
      }

      //  Insert data
      try (DuckDBAppender appender = ((DuckDBConnection) connection).createAppender("main", "test")) {
        for (int i = 0; i < numQueries; i++) {
          appender.beginRow();
          appender.append(i);
          appender.append(Math.random());
          appender.endRow();
        }
      }


      // Measure QPS
      int i = 0;
      Instant start = Instant.now();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT * FROM test where id >= 0 and id <=" + numQueries + "ORDER BY id")) {
        while (rs.next()) {
          int id = rs.getInt("id");
          double value = rs.getDouble("value");
          i++;
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      long qps = (long) numQueries / duration.toMillis() * 1000;
      System.out.println("Read QPS: " + qps + ", total data: " + i);

      // Clean up
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("DROP TABLE test");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}