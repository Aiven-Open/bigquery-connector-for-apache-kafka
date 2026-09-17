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

package com.wepay.kafka.connect.bigquery.write.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTaskTest;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
public class BigQueryWriterTest {
  private static SinkPropertiesFactory propertiesFactory;
  private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream =
      mock(StorageWriteApiDefaultStream.class);
  private static StorageApiBatchModeHandler mockedBatchHandler =
      mock(StorageApiBatchModeHandler.class);

  private final Time time = new MockTime();

  @BeforeAll
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void testBigQueryNoFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    // first attempt (success)
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testAutoCreateTables() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    String errorMessage = "Not found: Table project.scratch.test_topic";
    BigQueryError error = new BigQueryError("notFound", "global", errorMessage);
    BigQueryException nonExistentTableException = new BigQueryException(404, errorMessage, error);

    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(nonExistentTableException)
        .thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(schemaManager, times(1)).createTable(any(TableId.class), anyList());
    verify(schemaManager, never()).updateSchema(any(TableId.class), anyList());
    verify(bigQuery, times(2)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testSchemaUpdateAttemptedWhenRetryingAfterTableCreate() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);

    // The table does not exist, so the first attempt throws rather than reporting errors, which
    // means the write gets as far as creating the table but never updates its schema
    String errorMessage = "Not found: Table project.scratch.test_topic";
    BigQueryError error = new BigQueryError("notFound", "global", errorMessage);
    BigQueryException nonExistentTableException = new BigQueryException(404, errorMessage, error);

    // Once the table exists, it turns out to be missing a field that these rows have because a
    // different thread or task created the table, and createTable would have exited early.
    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors())
        .thenReturn(
            Collections.singletonMap(
                0L,
                Collections.singletonList(
                    new BigQueryError("invalid", "some_field", "no such field: some_field."))));

    // Finally, after we update schema, the insert would be expected to succeed.
    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(false);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(Collections.emptyMap());

    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(nonExistentTableException)
        .thenReturn(insertAllResponseWithError)
        .thenReturn(insertAllResponseNoError);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    // The first createTable call would have exited early due to a concurrent table write that
    // finished first.  That other table write would have omitted some_field in our example.
    verify(schemaManager, times(1)).createTable(any(TableId.class), anyList());
    // Thus we would have to attempt updating the schema on the new table.
    verify(schemaManager, times(1)).updateSchema(any(TableId.class), anyList());
    verify(bigQuery, times(3)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testSchemaUpdateNotAttemptedTwiceWhileRetrying() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");

    BigQuery bigQuery = mock(BigQuery.class);

    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors())
        .thenReturn(
            Collections.singletonMap(
                0L,
                Collections.singletonList(
                    new BigQueryError("invalid", "some_field", "no such field: some_field."))));

    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(false);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(Collections.emptyMap());

    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    // The first attempt reports the missing field, so the schema is updated before the retry loop
    // is even reached; the second attempt still fails because the update has yet to take effect.
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenReturn(insertAllResponseWithError)
        .thenReturn(insertAllResponseWithError)
        .thenReturn(insertAllResponseNoError);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    // Waiting for the update to take effect is not a reason to update again: every attempt would
    // derive the same schema from the same rows
    verify(schemaManager, never()).createTable(any(TableId.class), anyList());
    verify(schemaManager, times(1)).updateSchema(any(TableId.class), anyList());
    verify(bigQuery, times(3)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testDatasetNotFoundRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    String errorMessage = "Not found: Dataset project:scratch";
    BigQueryError error = new BigQueryError("notFound", "global", errorMessage);
    BigQueryException nonExistentDatasetException = new BigQueryException(404, errorMessage, error);

    // first attempt (dataset transiently not visible), second attempt (success)
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(nonExistentDatasetException)
        .thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(2)).insertAll(any(InsertAllRequest.class));
    verify(schemaManager, times(0)).createTable(any(TableId.class), anyList());
  }

  @Test
  public void testGatewayTimeoutRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    BigQueryException gatewayTimeoutException = new BigQueryException(504, "Gateway timeout");

    // first attempt (504 gateway timeout), second attempt (success)
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(gatewayTimeoutException)
        .thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(2)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testNonAutoCreateTables() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(insertAllResponse.hasErrors()).thenReturn(false);
    when(insertAllResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    BigQueryException missTableException = new BigQueryException(404, "Table is missing");

    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(missTableException)
        .thenReturn(insertAllResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    assertThrows(BigQueryConnectException.class, () -> testTask.flush(Collections.emptyMap()));
  }

  @Test
  public void testBigQueryPartialFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    BigQueryError insertError = new BigQueryError("reason", "location", "message");
    Map<Long, List<BigQueryError>> insertErrorMap =
        Collections.singletonMap(1L, Collections.singletonList(insertError));

    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors()).thenReturn(insertErrorMap);

    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(true);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(Collections.emptyMap());

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    // first attempt (partial failure); second attempt (success)
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenReturn(insertAllResponseWithError)
        .thenReturn(insertAllResponseNoError);

    List<SinkRecord> sinkRecordList = new ArrayList<>();
    sinkRecordList.add(spoofSinkRecord(topic, 0, 0, "some_field", "some_value"));
    sinkRecordList.add(spoofSinkRecord(topic, 1, 1, "some_field", "some_value"));

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(sinkRecordList);
    testTask.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> varArgs = ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, times(2)).insertAll(varArgs.capture());

    assertEquals(2, varArgs.getAllValues().get(0).getRows().size());
    // second insertAll is called with just the failed rows
    assertEquals(1, varArgs.getAllValues().get(1).getRows().size());
    assertEquals("test_topic-1-1", varArgs.getAllValues().get(1).getRows().get(0).getId());
  }

  @Test
  public void testBigQueryCompleteFailure() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    BigQueryError insertError = new BigQueryError("reason", "location", "message");

    Map<Long, List<BigQueryError>> insertErrorMap = new HashMap<>();
    insertErrorMap.put(1L, Collections.singletonList(insertError));
    insertErrorMap.put(2L, Collections.singletonList(insertError));

    InsertAllResponse insertAllResponseWithError = mock(InsertAllResponse.class);
    when(insertAllResponseWithError.hasErrors()).thenReturn(true);
    when(insertAllResponseWithError.getInsertErrors()).thenReturn(insertErrorMap);

    InsertAllResponse insertAllResponseNoError = mock(InsertAllResponse.class);
    when(insertAllResponseNoError.hasErrors()).thenReturn(true);
    when(insertAllResponseNoError.getInsertErrors()).thenReturn(Collections.emptyMap());

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    // first attempt (complete failure); second attempt (not expected)
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(insertAllResponseWithError);

    List<SinkRecord> sinkRecordList = new ArrayList<>();
    sinkRecordList.add(spoofSinkRecord(topic, 0, 0, "some_field", "some_value"));
    sinkRecordList.add(spoofSinkRecord(topic, 1, 1, "some_field", "some_value"));

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(sinkRecordList);
    Exception expectedEx =
        assertThrows(BigQueryConnectException.class, () -> testTask.flush(Collections.emptyMap()));
    assertTrue(expectedEx.getCause().getMessage().contains("test_topic"));
  }

  @Test
  public void testPutAttemptIdRefreshedOnInternalRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true");
    properties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "_kafka_data");

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse successResponse = mock(InsertAllResponse.class);
    when(successResponse.hasErrors()).thenReturn(false);
    when(successResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    // First call: backend error triggers internal retry inside BigQueryWriter.writeRows()
    BigQueryException backendError = new BigQueryException(500, "Internal server error");
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(backendError)
        .thenReturn(successResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, times(2)).insertAll(requestCaptor.capture());

    // Extract putAttemptId from each request's row
    String putAttemptIdFirst = extractPutAttemptId(requestCaptor.getAllValues().get(0));
    String putAttemptIdRetry = extractPutAttemptId(requestCaptor.getAllValues().get(1));

    assertTrue(
        putAttemptIdFirst != null && !putAttemptIdFirst.isEmpty(),
        "First attempt should have a putAttemptId");
    assertTrue(
        putAttemptIdRetry != null && !putAttemptIdRetry.isEmpty(),
        "Retry attempt should have a putAttemptId");
    assertNotEquals(
        putAttemptIdFirst,
        putAttemptIdRetry,
        "Internal retry must produce a different putAttemptId than the first attempt");
  }

  @Test
  public void testPutAttemptIdNotSetWhenTrackingDisabled() {
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    // trackPutAttempts defaults to false; set kafkaDataFieldName so the struct is included
    properties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "_kafka_data");

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse successResponse = mock(InsertAllResponse.class);
    when(successResponse.hasErrors()).thenReturn(false);
    when(successResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    BigQueryException backendError = new BigQueryException(500, "Internal server error");
    when(bigQuery.insertAll(any(InsertAllRequest.class)))
        .thenThrow(backendError)
        .thenReturn(successResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, times(2)).insertAll(requestCaptor.capture());

    // With tracking disabled, putAttemptId should not appear in the row content
    String putAttemptIdFirst = extractPutAttemptId(requestCaptor.getAllValues().get(0));
    String putAttemptIdRetry = extractPutAttemptId(requestCaptor.getAllValues().get(1));

    assertTrue(
        putAttemptIdFirst == null, "putAttemptId should be absent when trackPutAttempts=false");
    assertTrue(
        putAttemptIdRetry == null,
        "putAttemptId should be absent when trackPutAttempts=false on retry");
  }

  @Test
  public void testWriteAttemptIdPresentOnFirstAttempt() {
    // Even with no retry, writeRows() now generates a fresh write-attempt ID before the
    // first performWriteRequest() call, so the ID in BigQuery differs from the put-level ID.
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);
    properties.put(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true");
    properties.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "_kafka_data");

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);

    InsertAllResponse successResponse = mock(InsertAllResponse.class);
    when(successResponse.hasErrors()).thenReturn(false);
    when(successResponse.getInsertErrors()).thenReturn(Collections.emptyMap());

    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(successResponse);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Storage storage = mock(Storage.class);

    BigQuerySinkTask testTask =
        BigQuerySinkTaskTest.createTestTask(
            bigQuery,
            schemaRetriever,
            storage,
            schemaManager,
            mockedStorageWriteApiDefaultStream,
            mockedBatchHandler,
            time);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> requestCaptor =
        ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, times(1)).insertAll(requestCaptor.capture());

    String writeAttemptId = extractPutAttemptId(requestCaptor.getValue());
    assertTrue(
        writeAttemptId != null && !writeAttemptId.isEmpty(),
        "First (and only) write attempt should carry a write-attempt ID");
  }

  @SuppressWarnings("unchecked")
  private String extractPutAttemptId(InsertAllRequest request) {
    if (request.getRows().isEmpty()) {
      return null;
    }
    Map<String, Object> content = request.getRows().get(0).getContent();
    Object kafkaData = content.get("_kafka_data");
    if (!(kafkaData instanceof Map)) {
      return null;
    }
    Object id = ((Map<String, Object>) kafkaData).get("putAttemptId");
    return id != null ? id.toString() : null;
  }

  /**
   * Utility method for making and retrieving properties based on provided parameters.
   *
   * @param bigqueryRetry The number of retries.
   * @param bigqueryRetryWait The wait time for each retry.
   * @param topic The topic of the record.
   * @param dataset The dataset of the record.
   * @return The map of bigquery sink configurations.
   */
  private Map<String, String> makeProperties(
      String bigqueryRetry, String bigqueryRetryWait, String topic, String dataset) {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, bigqueryRetry);
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, bigqueryRetryWait);
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "6");
    return properties;
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   *
   * @param topic The topic of the record.
   * @param partition The partition of the record.
   * @param field The name of the field in the record's struct.
   * @param value The content of the field.
   * @return The spoofed SinkRecord.
   */
  private SinkRecord spoofSinkRecord(
      String topic, int partition, long kafkaOffset, String field, String value) {
    Schema basicRowSchema = SchemaBuilder.struct().field(field, Schema.STRING_SCHEMA).build();
    Struct basicRowValue = new Struct(basicRowSchema);
    basicRowValue.put(field, value);
    return new SinkRecord(
        topic, partition, null, null, basicRowSchema, basicRowValue, kafkaOffset, null, null);
  }
}
