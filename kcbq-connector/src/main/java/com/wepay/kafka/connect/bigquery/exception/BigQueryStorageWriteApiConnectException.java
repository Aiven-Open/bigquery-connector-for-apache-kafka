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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.exception;


import com.google.cloud.bigquery.storage.v1.RowError;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;

/**
 * Exception Class for exceptions that occur while interacting with BigQuery Storage Write API, such as login failures, schema
 * update failures, and table insertion failures.
 */
public class BigQueryStorageWriteApiConnectException extends ConnectException {

  public BigQueryStorageWriteApiConnectException(String message) {
    super(message);
  }

  public BigQueryStorageWriteApiConnectException(String message, Throwable error) {
    super(message, error);
  }

  public BigQueryStorageWriteApiConnectException(String tableName, List<RowError> errors) {
    super(formatRowErrors(tableName, errors));
  }

  public BigQueryStorageWriteApiConnectException(String tableName, Map<Integer, String> errors) {
    super(formatRowErrors(tableName, errors));
  }

  private static String formatRowErrors(String tableName, List<RowError> errors) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("Insertion failed at table %s for following rows: ", tableName));
    for (RowError error : errors) {
      builder.append(String.format(
          "\n [row index %d] (Failure reason : %s) ",
          error.getIndex(),
          error.getMessage())
      );
    }
    return builder.toString();
  }

  private static String formatRowErrors(String tableName, Map<Integer, String> errors) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("Insertion failed at table %s for following rows: ", tableName));
    for (Map.Entry<Integer, String> error : errors.entrySet()) {
      builder.append(String.format(
          "\n [row index %d] (Failure reason : %s) ",
          error.getKey(),
          error.getValue()
      ));
    }
    return builder.toString();
  }
}
