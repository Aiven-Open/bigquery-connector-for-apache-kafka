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

package com.wepay.kafka.connect.bigquery.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.TableId;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

public class PartitionedTableIdTest {

  @Test
  public void testBasicBuilder() {
    final String dataset = "dataset";
    final String table = "table";

    final PartitionedTableId tableId = new PartitionedTableId.Builder(dataset, table).build();

    assertEquals(dataset, tableId.getDataset());
    assertEquals(table, tableId.getBaseTableName());
    assertEquals(table, tableId.getFullTableName());

    TableId expectedTableId = TableId.of(dataset, table);
    assertEquals(expectedTableId, tableId.getBaseTableId());
    assertEquals(expectedTableId, tableId.getFullTableId());
  }

  @Test
  public void testTableIdBuilder() {
    final String project = "project";
    final String dataset = "dataset";
    final String table = "table";
    final TableId tableId = TableId.of(project, dataset, table);

    final PartitionedTableId partitionedTableId = new PartitionedTableId.Builder(tableId).build();

    assertEquals(project, partitionedTableId.getProject());
    assertEquals(dataset, partitionedTableId.getDataset());
    assertEquals(table, partitionedTableId.getBaseTableName());
    assertEquals(table, partitionedTableId.getFullTableName());

    assertEquals(tableId, partitionedTableId.getBaseTableId());
    assertEquals(tableId, partitionedTableId.getFullTableId());
  }

  @Test
  public void testWithPartition() {
    final String dataset = "dataset";
    final String table = "table";
    final LocalDate partitionDate = LocalDate.of(2016, 9, 21);

    final PartitionedTableId partitionedTableId =
        new PartitionedTableId.Builder(dataset, table).setDayPartition(partitionDate).build();

    final String expectedPartition = "20160921";

    assertEquals(dataset, partitionedTableId.getDataset());
    assertEquals(table, partitionedTableId.getBaseTableName());
    assertEquals(table + "$" + expectedPartition, partitionedTableId.getFullTableName());

    final TableId expectedBaseTableId = TableId.of(dataset, table);
    final TableId expectedFullTableId = TableId.of(dataset, table + "$" + expectedPartition);

    assertEquals(expectedBaseTableId, partitionedTableId.getBaseTableId());
    assertEquals(expectedFullTableId, partitionedTableId.getFullTableId());
  }

  @Test
  public void testWithEpochTimePartition() {
    final String dataset = "dataset";
    final String table = "table";

    final long utcTime = 1509007584334L;

    final PartitionedTableId partitionedTableId =
        new PartitionedTableId.Builder(dataset, table).setDayPartition(utcTime).build();

    final String expectedPartition = "20171026";

    assertEquals(dataset, partitionedTableId.getDataset());
    assertEquals(table, partitionedTableId.getBaseTableName());
    assertEquals(table + "$" + expectedPartition, partitionedTableId.getFullTableName());

    final TableId expectedBaseTableId = TableId.of(dataset, table);
    final TableId expectedFullTableId = TableId.of(dataset, table + "$" + expectedPartition);

    assertEquals(expectedBaseTableId, partitionedTableId.getBaseTableId());
    assertEquals(expectedFullTableId, partitionedTableId.getFullTableId());
  }
}
