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

package com.wepay.kafka.connect.bigquery.threads;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import java.util.List;
import java.util.Optional;

/** Common methods for all Executors */
public interface KcbqExecutor {

  /**
   * log the throwable and set the encounteredError.
   *
   * @param throwable the throwable to log and record.
   */
  void logAndRecord(Throwable throwable);

  /**
   * Immediately throw an exception if any unrecoverable errors were encountered by any of the write
   * tasks.
   *
   * @throws BigQueryConnectException if any of the tasks failed.
   */
  void maybeThrowEncounteredError();

  /**
   * Gets an optional throwable that contains the last error or is empty.
   *
   * @return an optional throwable that contains the last error or is empty.
   */
  Optional<Throwable> getLastError();

  /**
   * Gets the list of task names to in the queue.
   *
   * @return the list of task names to in the queue.
   */
  List<String> taskNames();

  /**
   * Gets the status of the queue.
   *
   * @return the status of the queue.
   */
  String status();
}
