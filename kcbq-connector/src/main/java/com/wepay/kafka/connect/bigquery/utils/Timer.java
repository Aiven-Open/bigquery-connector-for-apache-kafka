package com.wepay.kafka.connect.bigquery.utils;

import java.time.Duration;

/**
 * Simple timer utility used for retry handling.
 */
public class Timer {
  private final Time time;
  private final long deadlineMs;

  public Timer(final Time time, final Duration timeout) {
    this.time = time;
    this.deadlineMs = time.milliseconds() + timeout.toMillis();
  }

  /**
   * Returns {@code true} if the timer has expired.
   */
  public boolean isExpired() {
    return time.milliseconds() >= deadlineMs;
  }

  /**
   * Remaining time until the timer expires in milliseconds.
   */
  public long remainingMs() {
    return Math.max(0, deadlineMs - time.milliseconds());
  }
}