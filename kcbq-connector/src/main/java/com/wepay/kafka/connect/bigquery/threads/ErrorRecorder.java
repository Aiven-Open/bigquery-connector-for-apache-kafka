/*
 * Copyright 2026 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.threads;

import java.util.Collection;
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
   * Creates an ErrorLogger that simply collects the errors into a collection.
   *
   * @param collection the collection to put the error in.
   * @return an ErrorRecorder instance.
   */
  static ErrorRecorder from(Collection<Throwable> collection) {
    return new ErrorRecorder() {
      @Override
      public <T extends Throwable> T logAndRecord(T throwable) {
        collection.add(throwable);
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
