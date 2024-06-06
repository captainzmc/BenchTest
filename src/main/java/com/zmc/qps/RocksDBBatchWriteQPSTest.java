package com.zmc.qps;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

public class RocksDBBatchWriteQPSTest {

  public static void main(String[] args) {
    RocksDB.loadLibrary();
    String path = "/tmp/test_rocksdb";
    int numQueries = 1000000;
    int batchSize = 1000000;
    try {
      path = args[0];
      numQueries = Integer.parseInt(args[1]);
      batchSize = Integer.parseInt(args[2]);
    } catch (ArrayIndexOutOfBoundsException e) {
    }

    try (Options options = new Options().setCreateIfMissing(true);
         RocksDB db = RocksDB.open(options, path)) {

      // Measure write QPS
      Instant start = Instant.now();
      try (WriteBatch batch = new WriteBatch();
           WriteOptions writeOptions = new WriteOptions()) {
        for (int i = 0; i < numQueries; i++) {
          ByteBuffer keyBuffer = ByteBuffer.allocate(4);
          ByteBuffer valueBuffer = ByteBuffer.allocate(8);
          keyBuffer.putInt(i);
          valueBuffer.putDouble(Math.random());
          batch.put(keyBuffer.array(), valueBuffer.array());

          if (i % batchSize == 0 || i == numQueries - 1) {
            db.write(writeOptions, batch);
            batch.clear();
          }
        }
      }
      Instant end = Instant.now();
      Duration duration = Duration.between(start, end);
      double qps = (double) numQueries / duration.toMillis() * 1000;
      System.out.println("Write QPS: " + qps);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }
}
