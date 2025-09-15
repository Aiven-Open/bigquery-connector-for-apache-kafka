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

package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.exception.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.utils.Time;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles retries with writes to the Storage Write API and operations performed
 * during that process (such as table creation/update).
 */
public class StorageWriteApiRetryHandler {

  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiRetryHandler.class);
  private static final int ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE = 30;
  private static final int ADDITIONAL_RETRIES_WAIT_TABLE_CREATE_UPDATE = 30000;

  private final TableId table;
  private final List<SinkRecord> records;
  private final Random random;
  private final int userConfiguredRetry;
  private final long userConfiguredRetryWait;
  private final Time time;

  private BigQueryStorageWriteApiConnectException mostRecentException;
  private int additionalRetries;
  private int additionalWait;
  private int currentAttempt;

  /**
   * @deprecated Use {@link #StorageWriteApiRetryHandler(TableId, List, int, long, Time)} instead.
   */
  @Deprecated
  public StorageWriteApiRetryHandler(
          TableName table,
          List<SinkRecord> records,
          int retry,
          long retryWait,
          Time time
  ) {
    this(TableNameUtils.tableId(table), records, retry, retryWait, time);
  }

  public StorageWriteApiRetryHandler(
      TableId table,
      List<SinkRecord> records,
      int retry,
      long retryWait,
      Time time
  ) {
    additionalRetries = 0;
    additionalWait = 0;
    mostRecentException = null;
    currentAttempt = 0;
    this.table = table;
    this.records = records;
    this.userConfiguredRetry = retry;
    this.userConfiguredRetryWait = retryWait;
    this.time = time;
    this.random = new Random();
  }

  public BigQueryStorageWriteApiConnectException getMostRecentException() {
    return mostRecentException;
  }

  public void setMostRecentException(BigQueryStorageWriteApiConnectException mostRecentException) {
    this.mostRecentException = mostRecentException;
  }

  public int getAttempt() {
    return this.currentAttempt;
  }

  private void setAdditionalRetriesAndWait() {
    this.additionalRetries = ADDITIONAL_RETRIES_TABLE_CREATE_UPDATE;
    this.additionalWait = ADDITIONAL_RETRIES_WAIT_TABLE_CREATE_UPDATE;
  }

  private void waitRandomTime() throws InterruptedException {
    time.sleep(userConfiguredRetryWait + additionalWait + random.nextInt(1000));
  }

  public void maybeRetry(String operation) {
    if (currentAttempt < (userConfiguredRetry + additionalRetries)) {
      currentAttempt++;
      try {
        waitRandomTime();
      } catch (InterruptedException e) {
        logger.warn("Thread interrupted while waiting for random time");
      }
    } else {
      throw new BigQueryStorageWriteApiConnectException(
          String.format("Exceeded %s attempts to %s ", getAttempt(), operation),
          getMostRecentException()
      );
    }
  }

  /**
   * Attempts to create table
   *
   * @param tableOperation lambda of the table operation to perform
   */
  public void attemptTableOperation(BiConsumer<TableId, List<SinkRecord>> tableOperation) {
    try {
      tableOperation.accept(table, records);
      // Table takes time to be available for after creation
      setAdditionalRetriesAndWait();
    } catch (BigQueryException exception) {
      if (BigQueryErrorResponses.isRateLimitExceededError(exception)) {
        // Can happen if several tasks try to create a table all at once; should be fine
        logger.info("Table appears to have been created by a different task");
        setAdditionalRetriesAndWait();
        return;
      }
      throw new BigQueryStorageWriteApiConnectException(
          "Failed to create table " + table, exception);
    }
  }

}
