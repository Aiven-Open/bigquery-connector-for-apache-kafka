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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GcsToBqWriterTest {

  private static SinkPropertiesFactory propertiesFactory;

  private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream = mock(StorageWriteApiDefaultStream.class);
  private static StorageApiBatchModeHandler mockedBatchHandler = mock(StorageApiBatchModeHandler.class);

  private final Time time = new MockTime();

  @BeforeAll
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void testGCSNoFailure() {
    // test succeeding on first attempt
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    BigQuerySinkTask testTask = new BigQuerySinkTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
        cache,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(1)).create((BlobInfo) anyObject(), (byte[]) anyObject());
  }

  @Test
  public void testGCSSomeFailures() {
    // test failure through all configured retry attempts.
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    when(storage.create((BlobInfo) anyObject(), (byte[]) anyObject()))
        .thenThrow(new StorageException(500, "internal server error")) // throw first time
        .thenReturn(null); // return second time. (we don't care about the result.)

    BigQuerySinkTask testTask = new BigQuerySinkTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
        cache,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(2)).create((BlobInfo) anyObject(), (byte[]) anyObject());
  }

  @Test
  public void testGCSAllFailures() {
    // test failure through all configured retry attempts.
    final String topic = "test_topic";
    final String dataset = "scratch";
    final Map<String, String> properties = makeProperties("3", "2000", topic, dataset);

    BigQuery bigQuery = mock(BigQuery.class);
    expectTable(bigQuery);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    when(storage.create((BlobInfo) anyObject(), (byte[]) anyObject()))
        .thenThrow(new StorageException(500, "internal server error"));

    BigQuerySinkTask testTask = new BigQuerySinkTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
        cache,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    assertThrows(
        ConnectException.class,
        () -> testTask.flush(Collections.emptyMap())
    );
    // Budget = 3 * 2000ms = 6000ms → 2 sleeps → 3 total attempts
    verify(storage, times(3)).create((BlobInfo) anyObject(), (byte[]) anyObject());
  }

  @Test
  public void happyPathNoRetry() throws Exception {
    // retries/wait are irrelevant when everything succeeds first try
    int retries = 3;
    long retryWaitMs = 100;
    boolean autoCreate = false;

    // BigQuery: table lookup succeeds immediately
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    // GCS: upload succeeds on first call
    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

    SchemaManager schemaManager = mock(SchemaManager.class);
    Time mockTime = new MockTime();

    GcsToBqWriter writer =
        new GcsToBqWriter(
            storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate,false, mockTime);

    long t0 = mockTime.milliseconds();
    writer.writeRows(oneRow(), TableId.of("ds", "tbl"), "bucket", "blob");
    long elapsed = mockTime.milliseconds() - t0;

    // One lookup, one upload; no retries, no sleeps → elapsed should be 0
    verify(bigQuery, times(1)).getTable(any(TableId.class));
    verify(schemaManager, never()).updateSchema(any(TableId.class), anyList());
    verify(storage, times(1)).create(any(BlobInfo.class), any(byte[].class));
    verifyNoMoreInteractions(storage, bigQuery, schemaManager);
    assertEquals(0L, elapsed, "no backoff should occur on the happy path");
  }

  @Test
  public void schemaUpdateSkippedWhenEnabled() throws Exception {
    int retries = 1;
    long retryWaitMs = 100;
    boolean autoCreate = false;

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

    SchemaManager schemaManager = mock(SchemaManager.class);
    Time mockTime = new MockTime();

    GcsToBqWriter writer =
        new GcsToBqWriter(
            storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate, true, mockTime);

    writer.writeRows(oneRow(), TableId.of("ds", "tbl"), "bucket", "blob");

    verify(schemaManager, times(1)).updateSchema(any(TableId.class), anyList());
    verify(storage, times(1)).create(any(BlobInfo.class), any(byte[].class));
  }


  @Test
  public void backoffIsCapped() throws Exception {
    int retries = 4; // allow 3 sleeps then success
    long retryWaitMs = 5000; // 5s → sequence 5s, 10s, 10s (capped)
    boolean autoCreate = false;

    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class)))
        .thenThrow(new StorageException(500, "t1"))
        .thenThrow(new StorageException(500, "t2"))
        .thenThrow(new StorageException(500, "t3"))
        .thenReturn(null);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    SchemaManager schemaManager = mock(SchemaManager.class);
    Time mockTime = new MockTime();

    GcsToBqWriter writer =
        new GcsToBqWriter(
            storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate, true, mockTime);

    long t0 = mockTime.milliseconds();
    writer.writeRows(oneRow(), TableId.of("ds", "tbl"), "bucket", "blob");
    long elapsed = mockTime.milliseconds() - t0;

    long minExpected = 20_000; // Budget = retries(4) * retryWaitMs(5000) = 20s
    long maxExpected = minExpected + 3 * 1000; // + jitter bound
    verify(schemaManager, times(1)).updateSchema(any(TableId.class), anyList());
    verify(storage, times(4)).create(any(BlobInfo.class), any(byte[].class));
    assertTrue(elapsed >= minExpected, "elapsed too small: " + elapsed);
    assertTrue(elapsed <= maxExpected, "elapsed too large: " + elapsed);
  }

  @Test
  public void budgetCutsBeforeAllRetries() throws Exception {
    /*
     * In GcsToBqWriter.writeRows we compute:
     *   Duration timeout = Duration.ofMillis(Math.max(0L, retryWaitMs * Math.max(1, retries)));
     *
     * With retryWaitMs=100 and retries=100 → timeout ≈ 10_000 ms (our “budget”).
     * Exponential backoff sleeps (pre-cap) are ~100, 200, 400, 800, 1600, 3200, 6400...
     * The cumulative base waits cross ~10s around the 6th/7th sleep, so the budget should
     * cut off the loop *before* we consume all configured retries.
     */

    final int retries = 100; // very high; we want budget to be the stopping condition
    final long retryWaitMs = 100L; // base delay ⇒ budget = 100 * 100 = 10_000 ms
    final boolean autoCreate = false;

    // BigQuery always throws a retryable error so executeWithRetry keeps retrying until budget
    // ends.
    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class)))
        .thenThrow(new BigQueryException(500, "retryable backend error"));

    // Storage is never reached because we fail during table lookup.
    Storage storage = mock(Storage.class);

    SchemaManager schemaManager = mock(SchemaManager.class);
    Time mockTime = new MockTime(); // virtual clock; sleep() advances time but doesn’t block

    GcsToBqWriter writer =
        new GcsToBqWriter(
            storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate, true, mockTime);

    // Because lookup never succeeds and the time budget expires, writeRows should fail
    // with a BigQueryConnectException (null table interpreted as lookup failure).
    assertThrows(
        BigQueryConnectException.class,
        () -> writer.writeRows(oneRow(), TableId.of("ds", "tbl"), "bucket", "blob"));

    // We expect multiple getTable() attempts until budget expires.
    // Because jitter can vary up to 1s per sleep and sleeps are clamped by remaining budget,
    // the exact attempt count can vary a little. A stable range is ~6..10 total calls.
    verify(bigQuery, atLeast(6)).getTable(any(TableId.class));
    verify(bigQuery, atMost(10)).getTable(any(TableId.class));

    verify(schemaManager, never()).updateSchema(any(TableId.class), anyList());

    // No upload should be attempted since table resolution never succeeded.
    verify(storage, never()).create(any(BlobInfo.class), any(byte[].class));
  }

  private void expectTable(BigQuery mockBigQuery) {
    Table mockTable = mock(Table.class);
    when(mockBigQuery.getTable(anyObject())).thenReturn(mockTable);
  }

  /**
   * Utility method for making and retrieving properties based on provided parameters.
   *
   * @param bigqueryRetry     The number of retries.
   * @param bigqueryRetryWait The wait time for each retry.
   * @param topic             The topic of the record.
   * @param dataset           The dataset of the record.
   * @return The map of bigquery sink configurations.
   */
  private Map<String, String> makeProperties(String bigqueryRetry,
                                             String bigqueryRetryWait,
                                             String topic,
                                             String dataset) {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, bigqueryRetry);
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, bigqueryRetryWait);
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "9");
    // gcs config
    properties.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, topic);
    properties.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, "myBucket");
    return properties;
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   *
   * @param topic     The topic of the record.
   * @param partition The partition of the record.
   * @param field     The name of the field in the record's struct.
   * @param value     The content of the field.
   * @return The spoofed SinkRecord.
   */
  private SinkRecord spoofSinkRecord(String topic,
                                     int partition,
                                     long kafkaOffset,
                                     String field,
                                     String value) {
    Schema basicRowSchema = SchemaBuilder
        .struct()
        .field(field, Schema.STRING_SCHEMA)
        .build();
    Struct basicRowValue = new Struct(basicRowSchema);
    basicRowValue.put(field, value);
    return new SinkRecord(topic,
        partition,
        null,
        null,
        basicRowSchema,
        basicRowValue,
        kafkaOffset,
        null,
        null);
  }

  /**
   * Utility method that builds a minimal {@link SortedMap} containing exactly one {@link
   * SinkRecord} → {@link RowToInsert} mapping.
   *
   * <p>The map is ordered by Kafka offset via a {@link Comparator}, which is required because
   * {@link SinkRecord} does not implement {@link Comparable}. In this case the comparator is based
   * on {@link SinkRecord#kafkaOffset()}.
   *
   * <p>The single {@code SinkRecord} created here has:
   *
   * <ul>
   *   <li>topic {@code "t"}
   *   <li>partition {@code 0}
   *   <li>offset {@code 1}
   *   <li>a trivial schema with one field {@code "f"} of type {@code STRING}
   *   <li>a corresponding {@link Struct} value with {@code f = "v"}
   * </ul>
   *
   * The {@link RowToInsert} mirrors this content in a simple map ({@code {"f":"v"}}).
   *
   * <p>This helper is used in unit tests to supply a valid row set to {@link
   * GcsToBqWriter#writeRows}, without requiring a running Kafka cluster or real BigQuery/GCS
   * resources.
   *
   * @return a {@link SortedMap} with one synthetic record-to-row mapping
   */
  private SortedMap<SinkRecord, RowToInsert> oneRow() {
    // sorted by offset so TreeMap has a comparator
    Comparator<SinkRecord> byOffset = Comparator.comparingLong(SinkRecord::kafkaOffset);
    SortedMap<SinkRecord, RowToInsert> rows = new TreeMap<>(byOffset);

    Schema schema = SchemaBuilder.struct().field("f", Schema.STRING_SCHEMA).build();
    Struct value = new Struct(schema).put("f", "v");

    SinkRecord rec = new SinkRecord("t", 0, null, null, schema, value, 1L, null, null);
    Map<String, Object> content = new HashMap<>();
    content.put("f", "v");

    rows.put(rec, RowToInsert.of(content));
    return rows;
  }
}
