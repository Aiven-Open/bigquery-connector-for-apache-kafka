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

package com.wepay.kafka.connect.bigquery.integration.utils;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class BigQueryTestUtils {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryTestUtils.class);

  @Deprecated
  public static void createPartitionedTable(
      BigQuery bigQuery, String datasetName, String tableName, Schema schema) {
    try {
      TableId tableId = TableId.of(datasetName, tableName);

      TimePartitioning partitioning =
          TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build();

      StandardTableDefinition tableDefinition =
          StandardTableDefinition.newBuilder()
              .setSchema(schema)
              .setTimePartitioning(partitioning)
              .build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigQuery.create(tableInfo);
      logger.info("Partitioned table {} created successfully", tableName);
      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(bigQuery.getTable(tableId)).isNotNull());
    } catch (BigQueryException e) {
      logger.error("Failed to create partitioned table {} in dataset {}", tableName, datasetName);
      throw e;
    }
  }

  public static void createPartitionedTable(
          BigQuery bigQuery, TableName tableName, Schema schema) {
    try {
      TableId tableId = TableNameUtils.tableId(tableName);

      TimePartitioning partitioning =
              TimePartitioning.newBuilder(TimePartitioning.Type.DAY).build();

      StandardTableDefinition tableDefinition =
              StandardTableDefinition.newBuilder()
                      .setSchema(schema)
                      .setTimePartitioning(partitioning)
                      .build();
      TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

      bigQuery.create(tableInfo);
      logger.info("Partitioned table {} created successfully", tableName);
      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(bigQuery.getTable(tableId)).isNotNull());
    } catch (BigQueryException e) {
      fail("Failed to create partitioned table {}", tableName, e);
    }
  }

  public static void createStandardTable(
          BigQuery bigQuery, TableName tableName, Schema schema) {
    try {
      // Create the table...
      TableId tableId = TableNameUtils.tableId(tableName);
      bigQuery.create(TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build());
      logger.info("Standard table {} created successfully", tableName);
      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(bigQuery.getTable(tableId)).isNotNull());
    } catch (BigQueryException e) {
      fail("Failed to create standard table {}", tableName, e);
    }
  }
}
