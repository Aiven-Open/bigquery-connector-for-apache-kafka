package com.wepay.kafka.connect.bigquery.utils;

/**
 * Largely adapted from the
 * <a href="https://github.com/apache/kafka/blob/011d2382689df7b850b05281f4e19387c42c1f4d/clients/src/main/java/org/apache/kafka/common/utils/Time.java">Kafka Time interface</a>,
 * which is not public API and therefore cannot be relied upon as a dependency.
 */
public interface Time {

  void sleep(long durationMs) throws InterruptedException;

  long milliseconds();

  Time SYSTEM = new Time() {
    @Override
    public void sleep(long durationMs) throws InterruptedException {
      Thread.sleep(durationMs);
    }

    @Override
    public long milliseconds() {
      return System.currentTimeMillis();
    }
  };

}
