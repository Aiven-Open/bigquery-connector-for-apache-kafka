package com.wepay.kafka.connect.bigquery.utils;

import java.time.Duration;
import java.util.Random;

/**
 * Simple exponential backoff helper with jitter.
 */
public class Backoff {
  private static final int JITTER_MS = 1000;

  private final Time time;
  private final Random random = new Random();
  private final long maxDelayMs;
  private long currentDelayMs = 100L;

  public Backoff(final Time time, final Duration timeout) {
    this.time = time;
    this.maxDelayMs = timeout.toMillis();
  }

  /**
   * Sleep for the current backoff period and increase the delay exponentially.
   */
  public void delay() throws InterruptedException {
    long delay = Math.min(currentDelayMs, maxDelayMs);
    long jitter = random.nextInt(JITTER_MS);
    time.sleep(delay + jitter);
    currentDelayMs = Math.min(currentDelayMs * 2, maxDelayMs);
  }
}