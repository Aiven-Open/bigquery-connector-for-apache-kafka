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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
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
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApplicationStreamIT extends BaseConnectorIT {
  private static final Logger logger = LoggerFactory.getLogger(ApplicationStreamIT.class);
  String table = "applicationStreamTest";
  TableName tableName = TableName.of(project(), dataset(), table);
  String tableNameStr = tableName.toString();
  BigQueryWriteClient client;
  BigQueryWriteSettings writeSettings;
  JsonStreamWriterFactory jsonWriterFactory;
  private BigQuery bigQuery;

  @BeforeEach
  public void setup() throws IOException, InterruptedException {
    bigQuery = newBigQuery();
    createTable();
    writeSettings = new GcpClientBuilder.BigQueryWriteSettingsBuilder()
        .withProject(project())
        .withKeySource(GcpClientBuilder.KeySource.valueOf(keySource()))
        .withKey(keyFile())
        .withWriterApi(true)
        .build();
    client = BigQueryWriteClient.create(writeSettings);
    jsonWriterFactory = getJsonWriterFactory();
  }

  @Test
  public void testStreamCreation() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    assertEquals(applicationStream.getCurrentState(), StreamState.CREATED);
    assertNotNull(applicationStream.writer());
    applicationStream.closeStream();
  }

  @Test
  public void testStreamClose() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    String streamName = applicationStream.writer().getStreamName();
    applicationStream.closeStream();
    assertNotEquals(applicationStream.writer().getStreamName(), streamName);
  }

  @Test
  public void testApplicationStreamName() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    assertTrue(applicationStream.getStreamName().contains("streams"));
    applicationStream.closeStream();
  }

  @Test
  public void testMaxCallCount() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    assertEquals(applicationStream.getCurrentState(), StreamState.CREATED);
    int maxCount = applicationStream.increaseMaxCalls();
    assertEquals(applicationStream.getCurrentState(), StreamState.APPEND);
    assertEquals(1, maxCount);
    applicationStream.closeStream();
  }

  @Test
  public void testCanBeMovedToNonActive() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    assertFalse(applicationStream.canTransitionToNonActive());
    applicationStream.increaseMaxCalls();
    assertTrue(applicationStream.canTransitionToNonActive());
    applicationStream.closeStream();
  }

  @Test
  public void testResetWriter() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    JsonStreamWriter writer = applicationStream.writer();
    applicationStream.closeStream();
    JsonStreamWriter updatedWriter = applicationStream.writer();
    assertNotEquals(writer, updatedWriter);
    applicationStream.closeStream();
  }

  @Test
  public void testStreamFinalised() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    applicationStream.increaseMaxCalls();
    applicationStream.closeStream();
    applicationStream.writer();
    assertEquals(applicationStream.getCurrentState(), StreamState.APPEND);
    applicationStream.finalise();
    assertEquals(applicationStream.getCurrentState(), StreamState.FINALISED);
    applicationStream.closeStream();
  }

  @Test
  public void testStreamCommitted() throws Exception {
    ApplicationStream applicationStream = new ApplicationStream(tableNameStr, client, jsonWriterFactory);
    applicationStream.increaseMaxCalls();
    applicationStream.closeStream();
    applicationStream.writer();
    applicationStream.finalise();
    assertEquals(applicationStream.getCurrentState(), StreamState.FINALISED);
    applicationStream.commit();
    assertEquals(applicationStream.getCurrentState(), StreamState.COMMITTED);
    applicationStream.closeStream();
  }

  private void createTable() throws InterruptedException {
    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, dataset(), table, null);
      int attempts = 10;
      while (bigQuery.getTable(TableNameUtils.tableId(tableName)) == null && attempts > 0) {
        logger.debug("Busy waiting for table {} to appear! Attempt {}", table, (10 - attempts));
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        attempts--;
      }
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate"))
        throw new ConnectException("Failed to create table: ", ex);
      else
        logger.info("Table {} already exist", table);
    }
  }

  private JsonStreamWriterFactory getJsonWriterFactory() {
    return streamOrTableName -> JsonStreamWriter.newBuilder(streamOrTableName, client).build();
  }
}
