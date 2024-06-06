package com.zmc.qps;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

public class RocksDBScanQPSTest {

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
          ByteBuffer valueBuffer = ByteBuffer.allocate(8);
          keyBuffer.putInt(i);
          valueBuffer.putDouble(Math.random());
          db.put(keyBuffer.array(), valueBuffer.array());
        }

        // Measure QPS
        int count = 0;
        Instant start = Instant.now();
        try (RocksIterator iterator = db.newIterator()) {
          for (int i = 0; i < numQueries; i++) {
            ByteBuffer keyBuffer = ByteBuffer.allocate(4);
            keyBuffer.putInt(i);
            iterator.seek(keyBuffer.array());
            if (iterator.isValid()) {
              byte[] keyBytes = iterator.key();
              byte[] valueBytes = iterator.value();
              ByteBuffer valueBuffer = ByteBuffer.wrap(valueBytes);
              double value = valueBuffer.getDouble();
              count ++;
            }
          }
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        long qps = (long) numQueries / duration.toMillis() * 1000;
        System.out.println("Read QPS: " + qps + ", total data: " + count);
      } catch (RocksDBException e) {
        e.printStackTrace();
      }
    }
}

