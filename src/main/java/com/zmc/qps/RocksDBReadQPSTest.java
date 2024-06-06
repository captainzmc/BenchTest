package com.zmc.qps;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

public class RocksDBReadQPSTest {

  public static void main(String[] args) {
    RocksDB.loadLibrary();
    String path = "/tmp/test_rocksdb";
    int numQueries = 1000000;
    try {
      path = args[0];
      numQueries = Integer.parseInt(args[1]);
    } catch (ArrayIndexOutOfBoundsException e) {
    }

    try (Options options = new Options().setCreateIfMissing(true);
         RocksDB db = RocksDB.open(options, path)) {

      // Insert some data
      for (int i = 0; i < numQueries; i++) {
        ByteBuffer keyBuffer = ByteBuffer.allocate(4);
        ByteBuffer valueBuffer = ByteBuffer.allocate(12);
        keyBuffer.putInt(i);
        valueBuffer.putInt(i).putDouble(Math.random());
        db.put(keyBuffer.array(), valueBuffer.array());
      }

      // Measure read QPS
      Instant start = Instant.now();
      for (int i = 0; i < numQueries; i++) {
        ByteBuffer keyBuffer = ByteBuffer.allocate(4);
        keyBuffer.putInt(i);
        byte[] valueBytes = db.get(keyBuffer.array());
        if (valueBytes != null) {
          ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
          int id = valueBuffer.getInt();
          double value = valueBuffer.getDouble();
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double qps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Read QPS: " + qps);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }
}
