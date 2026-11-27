package com.wepay.kafka.connect.bigquery.threads;

import org.slf4j.Logger;

@FunctionalInterface
public interface ErrorRecorder {

  /**
   * Creates an ErrorLogger that simply logs the error.
   *
   * @param logger the logger to log the error to.
   * @return an ErrorRecorder instance.
   */
  static ErrorRecorder from(Logger logger) {
    return new ErrorRecorder() {
      @Override
      public <T extends Throwable> T logAndRecord(T throwable) {
        logger.warn("Error in runnable", throwable);
        return throwable;
      }
    };
  }

  /**
   * logs the throwable and alters internal state based on the error.
   *
   * @param throwable the throwable to log and record.
   * @return the throwable argument;
   */
  <T extends Throwable> T logAndRecord(T throwable);
}
