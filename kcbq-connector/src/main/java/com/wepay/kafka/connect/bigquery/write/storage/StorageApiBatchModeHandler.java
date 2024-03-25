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

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles all operations related to Batch Storage Write API
 */
public class StorageApiBatchModeHandler {

  private static final Logger logger = LoggerFactory.getLogger(StorageApiBatchModeHandler.class);
  private final StorageWriteApiBatchApplicationStream streamApi;

  public StorageApiBatchModeHandler(StorageWriteApiBatchApplicationStream streamApi, BigQuerySinkTaskConfig config) {
    this.streamApi = streamApi;
  }

  /**
   * Used by the scheduler to commit all eligible streams and create new active
   * streams.
   */
  public void refreshStreams() {
    logger.trace("Storage Write API commit stream attempt by scheduler");
    streamApi.refreshStreams();
  }

  /**
   * Saves the offsets assigned to a particular stream on a table. This is required to commit offsets sequentially
   * even if the execution takes place in parallel at different times.
   *
   * @param tableName Name of tha table in project/dataset/tablename format
   * @param rows      Records which would be written to table {tableName} sent to define schema if table creation is
   *                  attempted
   * @return Returns the streamName on which offsets are updated
   */
  public String updateOffsetsOnStream(
      String tableName,
      List<ConvertedRecord> rows) {
    logger.trace("Updating offsets on current stream of table {}", tableName);
    return this.streamApi.updateOffsetsOnStream(tableName, rows);
  }

  /**
   * Gets offsets which are committed on BigQuery table.
   *
   * @return Returns Map of topic, partition, offset mapping
   */
  public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
    logger.trace("Getting list of commitable offsets for batch mode");
    return this.streamApi.getCommitableOffsets();
  }

}
