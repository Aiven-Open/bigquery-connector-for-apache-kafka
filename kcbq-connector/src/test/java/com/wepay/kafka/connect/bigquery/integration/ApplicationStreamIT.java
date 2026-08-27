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

package com.wepay.kafka.connect.bigquery.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.storage.ApplicationStream;
import com.wepay.kafka.connect.bigquery.write.storage.JsonStreamWriterFactory;
import com.wepay.kafka.connect.bigquery.write.storage.StreamState;

import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ApplicationStreamIT extends BaseConnectorIT {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationStreamIT.class);
  private BigQueryWriteClient client;
    private BigQuery bigQuery;
  private ApplicationStream underTest;

  @BeforeEach
  void setup() throws Exception {
    bigQuery = newBigQuery();
    createTable();
      BigQueryWriteSettings writeSettings = new GcpClientBuilder.BigQueryWriteSettingsBuilder()
              .withProject(project())
              .withKeySource(GcpClientBuilder.KeySource.valueOf(keySource()))
              .withKey(keyFile())
              .withWriterApi(true)
              .build();
    client = BigQueryWriteClient.create(writeSettings);
      JsonStreamWriterFactory jsonWriterFactory = getJsonWriterFactory();
    underTest =  new ApplicationStream(tableName().toString(), client, jsonWriterFactory);
  }

  @AfterEach
  void teardown() {
    underTest.closeStream();
    TableName tableName = tableName();
    bigQuery.delete(TableId.of(tableName.getDataset(), tableName.getProject(), tableName.getTable()));
  }


  @Test
  void testStreamCreation() {
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.CREATED);
    assertThat(underTest.writer()).isNotNull();
    underTest.closeStream();
  }

  @Test
  void testStreamClose() {
    String streamName = underTest.writer().getStreamName();
    underTest.closeStream();
    assertThat(underTest.writer().getStreamName()).isNotEqualTo(streamName);
  }

  @Test
  void testApplicationStreamName() {
    assertThat(underTest.getStreamName()).contains("streams");
    underTest.closeStream();
  }

  @Test
  void testMaxCallCount() {
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.CREATED);
    int maxCount = underTest.increaseMaxCalls();
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.APPEND);
    assertThat(maxCount).isEqualTo(1);
    underTest.closeStream();
  }

  @Test
  void testCanBeMovedToNonActive() {
    assertThat(underTest.canTransitionToNonActive()).isFalse();
    underTest.increaseMaxCalls();
    assertThat(underTest.canTransitionToNonActive()).isTrue();
    underTest.closeStream();
  }

  @Test
  void testResetWriter() {
    JsonStreamWriter writer = underTest.writer();
    underTest.closeStream();
    JsonStreamWriter updatedWriter = underTest.writer();
    assertThat(updatedWriter).isNotEqualTo(writer);
    underTest.closeStream();
  }

  @Test
  void testStreamFinalised() {
    underTest.increaseMaxCalls();
    underTest.closeStream();
    underTest.writer();
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.APPEND);
    underTest.finalise();
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.FINALISED);
    underTest.closeStream();
  }

  @Test
  void testStreamCommitted() {
    underTest.increaseMaxCalls();
    underTest.closeStream();
    underTest.writer();
    underTest.finalise();
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.FINALISED);
    underTest.commit();
    assertThat(underTest.getCurrentState()).isEqualTo(StreamState.COMMITTED);
    underTest.closeStream();
  }

  private void createTable() {
    TableName tableName = tableName();
    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, tableName.getDataset(), tableName.getTable(), null);
      Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(bigQuery.getTable(TableNameUtils.tableId(tableName))).isNotNull());
    } catch (BigQueryException ex) {
      if (ex.getError() != null && !ex.getError().getReason().equalsIgnoreCase("duplicate")) {
        fail("Failed to create table " + tableName.getTable(), ex);
      } else { logger.info("Table {} already exist", tableName.getTable()); }
    }
  }

  private JsonStreamWriterFactory getJsonWriterFactory() {
    return streamOrTableName -> JsonStreamWriter.newBuilder(streamOrTableName, client).build();
  }
}
