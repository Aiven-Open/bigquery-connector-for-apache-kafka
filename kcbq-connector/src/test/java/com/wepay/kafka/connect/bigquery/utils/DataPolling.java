/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
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
package com.wepay.kafka.connect.bigquery.utils;

import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to poll a task and wait for specific results. */
public final class DataPolling {

  private static final Logger logger = LoggerFactory.getLogger(DataPolling.class);

  private DataPolling() {
    // do not instantiate
  }

  /**
   * A predicate that is {@code true} as soon as some data appears. Used for {@link
   * #pollUntilResults(BigQuerySinkTask, Predicate)} and similar methods.
   */
  public static final Predicate<Map<TopicPartition, OffsetAndMetadata>> SOME_DATA =
      m -> !m.isEmpty();

  /**
   * Creates predicate that is {@code true} as soon as the specified number of records appear. Used
   * for {@link #pollUntilResults(BigQuerySinkTask, Predicate)} and similar methods.
   *
   * @param count the number of records to wait for.
   * @return a properly configured Predicate.
   */
  public static Predicate<Map<TopicPartition, OffsetAndMetadata>> recordCount(int count) {
    return m -> m.size() == count;
  }

  /**
   * Polls {@link BigQuerySinkTask#preCommit(Map)} for a maximum of 5 seconds until the exception is
   * thrown.
   *
   * @param testTask the BigQuerySinkTask to poll.
   * @param exceptionType the expected Exception class.
   * @return the thrown exception.
   */
  public static Exception pollUntilException(
      BigQuerySinkTask testTask, Class<? extends Exception> exceptionType) {
    return pollUntilException(Duration.ofSeconds(5), testTask, exceptionType);
  }

  /**
   * Polls {@link BigQuerySinkTask#preCommit(Map)} for a maximum of the specified duration until the
   * exception is thrown.
   *
   * @param atMost the maximum duration to wait for the exception.
   * @param testTask the BigQuerySinkTask to poll.
   * @param exceptionType the expected Exception class.
   * @return the thrown exception.
   */
  public static Exception pollUntilException(
      Duration atMost, BigQuerySinkTask testTask, Class<? extends Exception> exceptionType) {
    return pollUntilException(
        atMost, () -> testTask.preCommit(Collections.emptyMap()), exceptionType);
  }

  /**
   * Polls the specified command for a maximum of the specified duration until the exception is
   * thrown.
   *
   * @param atMost the maximum duration to wait for the exception.
   * @param cmd the specified runnable to poll.
   * @param exceptionType the expected Exception class.
   * @return the thrown exception.
   */
  public static Exception pollUntilException(
      Duration atMost, Runnable cmd, Class<? extends Exception> exceptionType) {
    Exception[] result = new Exception[1];
    Awaitility.waitAtMost(atMost)
        .pollInterval(Duration.ofSeconds(1))
        .alias("Exception not thrown")
        .until(
            () -> {
              try {
                cmd.run();
                return false;
              } catch (Exception e) {
                if (exceptionType.isAssignableFrom(e.getClass())) {
                  result[0] = e;
                  return true;
                }
                logger.warn("Wrong exception: ", e);
                throw e;
              }
            });
    return result[0];
  }

  /**
   * Polls the testTask {@link BigQuerySinkTask#preCommit(Map)} for a maximum of 5 seconds waiting
   * for the isDone predicate to be true.
   *
   * @param testTask the task to test.
   * @param isDone the predicate that should be true.
   * @return the data that matched the predicate.
   */
  public static Map<TopicPartition, OffsetAndMetadata> pollUntilResults(
      BigQuerySinkTask testTask, Predicate<Map<TopicPartition, OffsetAndMetadata>> isDone) {
    return pollUntilResults(Duration.ofSeconds(5), testTask, isDone);
  }

  /**
   * Polls the testTask {@link BigQuerySinkTask#preCommit(Map)} for the maximum time specified
   * waiting for the isDone predicate to be true.
   *
   * @param atMost the maximum time to wait for the predicate to be true.
   * @param testTask the task to test.
   * @param isDone the predicate that should be true.
   * @return the data that matched the predicate.
   */
  public static Map<TopicPartition, OffsetAndMetadata> pollUntilResults(
      Duration atMost,
      BigQuerySinkTask testTask,
      Predicate<Map<TopicPartition, OffsetAndMetadata>> isDone) {
    Map<TopicPartition, OffsetAndMetadata> results = new HashMap<>();
    Awaitility.waitAtMost(atMost)
        .pollInterval(Duration.ofSeconds(1))
        .alias("Records were not located")
        .until(
            () -> {
              results.putAll(testTask.preCommit(Collections.emptyMap()));
              return isDone.test(results);
            });
    return results;
  }
}
