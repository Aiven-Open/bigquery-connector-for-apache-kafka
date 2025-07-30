/*
 * Copyright 2025 Aiven Oy and bigquery-connector-for-apache-kafka project contributors
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

package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Class for resolving a {@link PartitionedTableId PartitionedTableId} of a {@link SinkRecord SinkRecord}
 */
class RecordTableResolver {
  private final BigQuerySinkTaskConfig config;
  private final BigQuery bigQuery;
  private final MergeBatches mergeBatches;
  private final Map<String, TableId> topicToTableId = new ConcurrentHashMap<>();
  private final boolean usePartitionDecorator;
  private final boolean upsertDelete;
  private final boolean useMessageTimeDatePartitioning;

  public RecordTableResolver(BigQuerySinkTaskConfig config, MergeBatches mergeBatches, BigQuery bigQuery,
                             boolean upsertDelete, boolean useStorageApiBatchMode) {
    this.config = config;
    this.mergeBatches = mergeBatches;
    this.bigQuery = bigQuery;

    this.upsertDelete = upsertDelete;
    this.useMessageTimeDatePartitioning = config.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
    this.usePartitionDecorator = !useStorageApiBatchMode && config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG);
  }

  public PartitionedTableId getRecordTable(SinkRecord record) {
    TableId baseTableId = getBaseTableId(record.topic());
    if (upsertDelete) {
      TableId intermediateTableId = mergeBatches.intermediateTableFor(baseTableId);
      // If upsert/delete is enabled, we want to stream into a non-partitioned intermediate table
      return new PartitionedTableId.Builder(intermediateTableId).build();
    }

    PartitionedTableId.Builder builder = new PartitionedTableId.Builder(baseTableId);
    if (usePartitionDecorator) {
      if (useMessageTimeDatePartitioning) {
        if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
          throw new ConnectException("Message has no timestamp type, cannot use message timestamp to partition.");
        }
        builder.setDayPartition(record.timestamp());
      } else {
        builder.setDayPartitionForNow();
      }
    }
    return builder.build();
  }

  private TableId getBaseTableId(String topic) {
    return topicToTableId.computeIfAbsent(topic, topicName -> {
      String[] datasetAndTable = TableNameUtils.getDataSetAndTableName(config, topic);
      String project = config.getString(BigQuerySinkConfig.PROJECT_CONFIG);
      TableId baseTableId = TableId.of(project, datasetAndTable[0], datasetAndTable[1]);

      if (usePartitionDecorator) {
        validatePartitioningForDecorator(baseTableId);
      }

      return baseTableId;
    });
  }

  private void validatePartitioningForDecorator(TableId tableId) {
    StandardTableDefinition definition = retrieveTableDefinition(tableId);
    if (definition == null) {
      // If we could not find table and its definition, ignore.
      // Table creation will potentially be handled later if auto create is enabled
      return;
    }
    TimePartitioning partitioning = definition.getTimePartitioning();
    if (partitioning == null) {
      throw new ConnectException(String.format(
              "Cannot use decorator syntax to write to %s as it is not partitioned",
              TableNameUtils.table(tableId)
      ));
    }
    if (partitioning.getType() != TimePartitioning.Type.DAY) {
      throw new ConnectException(String.format(
              "Cannot use decorator syntax to write to %s as it is partitioned by %s and not by day",
              TableNameUtils.table(tableId),
              partitioning.getType().name().toLowerCase()
      ));
    }
  }

  private StandardTableDefinition retrieveTableDefinition(TableId tableId) {
    try {
      Table table = bigQuery.getTable(tableId);
      return table == null ? null : table.getDefinition();
    } catch (BigQueryException e) {
      if (BigQueryErrorResponses.isAuthenticationError(e)) {
        throw new BigQueryConnectException("Failed to authenticate client for table " + tableId + " with error " + e, e);
      } else if (BigQueryErrorResponses.isIoError(e)) {
        throw new RetriableException("Failed to retrieve information for table " + tableId, e);
      } else {
        throw e;
      }
    }
  }
}