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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.bigquery.TableInfo;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTaskTest;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.GsonUtils;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;


import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Base64;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.debezium.data.VariableScaleDecimal;
import org.mockito.stubbing.OngoingStubbing;

public class GcsToBqWriterTest {

  private static SinkPropertiesFactory propertiesFactory;

  private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream = mock(StorageWriteApiDefaultStream.class);
  private static StorageApiBatchModeHandler mockedBatchHandler = mock(StorageApiBatchModeHandler.class);

  private static final TableId tableId = TableId.of("ds", "tbl");
  private static final Table table = mock(Table.class);

  private final Time time = new MockTime();

  @BeforeAll
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
    when(table.getTableId()).thenReturn(tableId);

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

    BigQuerySinkTask testTask = BigQuerySinkTaskTest.createTestTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(1)).create(any(BlobInfo.class), any(byte[].class));
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

    when(storage.create(any(BlobInfo.class), any(byte[].class)))
        .thenThrow(new StorageException(500, "internal server error")) // throw first time
        .thenReturn(null); // return second time. (we don't care about the result.)

    BigQuerySinkTask testTask = BigQuerySinkTaskTest.createTestTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
    testTask.put(
        Collections.singletonList(spoofSinkRecord(topic, 0, 0, "some_field", "some_value")));
    testTask.flush(Collections.emptyMap());

    verify(storage, times(2)).create(any(BlobInfo.class), any(byte[].class));
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

    when(storage.create(any(BlobInfo.class), any(byte[].class)))
        .thenThrow(new StorageException(500, "internal server error"));

    BigQuerySinkTask testTask = BigQuerySinkTaskTest.createTestTask(
        bigQuery,
        schemaRetriever,
        storage,
        schemaManager,
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
    verify(storage, times(3)).create(any(BlobInfo.class), any(byte[].class));
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
            storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate, false, mockTime);

    long t0 = mockTime.milliseconds();
    writer.writeRows(oneRow(), tableId, "bucket", "blob");
    long elapsed = mockTime.milliseconds() - t0;

    // One lookup, one upload; no retries, no sleeps → elapsed should be 0
    verify(bigQuery, times(1)).getTable(any(TableId.class));
    verify(schemaManager, never()).updateSchema(any(TableId.class), anyList());
    verify(storage, times(1)).create(any(BlobInfo.class), any(byte[].class));
    verifyNoMoreInteractions(storage, bigQuery, schemaManager);
    assertEquals(0L, elapsed, "no backoff should occur on the happy path");
  }

  @Test
  public void schemaUpdatedWhenEnabled() throws Exception {
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

    writer.writeRows(oneRow(), tableId, "bucket", "blob");

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
    writer.writeRows(oneRow(), tableId, "bucket", "blob");
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
        () -> writer.writeRows(oneRow(), tableId, "bucket", "blob"));

    // We expect multiple getTable() attempts until budget expires.
    // Because jitter can vary up to 1s per sleep and sleeps are clamped by remaining budget,
    // the exact attempt count can vary a little. A stable range is ~6..10 total calls.
    verify(bigQuery, atLeast(6)).getTable(any(TableId.class));
    verify(bigQuery, atMost(10)).getTable(any(TableId.class));

    verify(schemaManager, never()).updateSchema(any(TableId.class), anyList());

    // No upload should be attempted since table resolution never succeeded.
    verify(storage, never()).create(any(BlobInfo.class), any(byte[].class));
  }


    /**
     * A mocked SchemaManager.
     * @param createTable value for {@code createTable()} call.  null == exception, else value.
     * @param schemaUpdate value for {@code updateSchema()} call. null == exception, false = BQException, true = success
     * @return A mock schema manager
     */
  private SchemaManager mockSchemaManager(Boolean createTable, Boolean schemaUpdate) {
      SchemaManager schemaManager = mock(SchemaManager.class);

      if (createTable == null) {
          doThrow(new IllegalArgumentException("SchemaManager create table failed")).when(schemaManager).createTable(eq(tableId), anyList());
      } else {
          when(schemaManager.createTable(eq(tableId), anyList())).thenReturn(createTable);
      }

      if (schemaUpdate == null) {
          doThrow(new UnsupportedOperationException("SchemaManager threw exception")).when(schemaManager).updateSchema(any(), anyList());
      } else if (!schemaUpdate) {
          doThrow(new BigQueryConnectException("SchemaManager schema update failed")).when(schemaManager).updateSchema(any(), anyList());
      }
      return schemaManager;
  }

    /**
     * Create a mocked big query.
     * @param falseCount the number of times to report the files is not found.
     * @param hasTable if true a table is returned on the falseCount + 1 request.
     * @return a mocked BigQuery.
     */
    private BigQuery mockBigQuery(int falseCount, boolean hasTable) {
        BigQuery bigQuery = mock(BigQuery.class);
        OngoingStubbing<Table> stub = when(bigQuery.getTable(eq(tableId)));
        for (int i = 0; i < falseCount; i++) {
            stub = stub.thenReturn(null);
        }
        if (hasTable) {
            stub.thenReturn(table);
        }

        return bigQuery;
    }


    /**
     * A mocked Storage.
     * @param retryError null = no error, true = retryable error, false = non-retryable error.
     * @return a mocked storage that succeeds or fails beased on retryError flag.
     */
    private Storage mockStorage(Boolean retryError) {
        Storage storage = mock(Storage.class);
        if (retryError != null) {
            StorageException storageException = retryError ? new StorageException(500, "it failed") : new StorageException(400, "it failed");
            when(storage.create(any(BlobInfo.class), any(byte[].class))).thenThrow(storageException);
        }
        return storage;
    }


    @Test
  void writeRowsCreateTableTest() {
      final int retries = 4;
      final long retryWaitMs = 100L;
      final boolean attemptSchemaUpdate = false;

      Time mockTime = new MockTime(); // virtual clock; sleep() advances time but doesn’t block

      // BigQuery does not have the table, schema manager should not be called, any call the schema manager will result in an exception.
      String msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, false), mockSchemaManager(null, null),
                      retries, retryWaitMs, false, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "no table, schema manager exception"
      ).getMessage();
      assertEquals("Failed to lookup table " + tableId, msg);

      // BigQuery does not have the table.  Schema manager will return true
      msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, false), mockSchemaManager(true, null),
                      retries, retryWaitMs, true, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "no table, schema manager reports success").getMessage();
      assertEquals("Failed to lookup table " + tableId, msg);

      // BigQuery does not have the table.  Schema manager will return false
      msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, false), mockSchemaManager(false, null),
                      retries, retryWaitMs, true, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "no table, schema manager reports failure").getMessage();
      assertEquals("Failed to lookup table " + tableId, msg);

      // BigQuery does not have the table, schema manager will throw an exception.
      msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, false), mockSchemaManager(null, null),
                      retries, retryWaitMs, true, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "no table, schema manager exception"
      ).getMessage();
      assertEquals("Operation failed during executeWithRetry: SchemaManager create table failed", msg);

      // BigQuery does not have the table on the first call, but will on second call.  Schema manager will return false
      assertDoesNotThrow(() ->
                      new GcsToBqWriter(mockStorage(null), mockBigQuery(1, true), mockSchemaManager(false, null),
                              retries, retryWaitMs, true, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "not table then table, schema manager did not create table");

      assertDoesNotThrow(() -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, true), mockSchemaManager(true, null),
                      retries, retryWaitMs, true, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "not table then table, schema manager did create table");

      // BigQuery does not have the table on the first call, but will on second call.  Schema manager should not be called, any call the schema manager will result in an exception.
      assertDoesNotThrow(() -> new GcsToBqWriter(mockStorage(null), mockBigQuery(1, true), mockSchemaManager(null, null),
                      retries, retryWaitMs, false, attemptSchemaUpdate, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
              "not table then table, schema manager exception");

  }

    @Test
    void writeRowsUpdateSchemaTest() {
        final int retries = 4;
        final long retryWaitMs = 100L;

        Time mockTime = new MockTime(); // virtual clock; sleep() advances time but doesn’t block

        // schema update throws exception.
        String msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(0, true),
                        mockSchemaManager(null, null),
                        retries, retryWaitMs, false, true, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "schema manager update failed"
        ).getMessage();
        assertEquals("Operation failed during executeWithRetry: SchemaManager threw exception", msg);

        // schema update returns false
        msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(null), mockBigQuery(0, true),
                        mockSchemaManager(null, false),
                        retries, retryWaitMs, false, true, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "schema manager update failed"
        ).getMessage();
        assertEquals("Operation failed during executeWithRetry: SchemaManager schema update failed", msg);

        final SchemaManager schemaManager = mock(SchemaManager.class);
        doThrow(new BigQueryException(500, "it failed")).when(schemaManager).updateSchema(any(), anyList());
        msg = assertThrows(BigQueryConnectException.class,() -> new GcsToBqWriter(mockStorage(null), mockBigQuery(0, true), schemaManager,
                        retries, retryWaitMs, false, true, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "schema manager update faild with timeout").getMessage();
        assertTrue(msg.startsWith("Timeout expired after "));

        // schema update returns true
        assertDoesNotThrow(() -> new GcsToBqWriter(mockStorage(null), mockBigQuery(0, true), mockSchemaManager(null, true),
                        retries, retryWaitMs, false, true, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "schema manager update succeeded"
        );
    }

    @Test
    void writeRowsUploadData() {
        final int retries = 4;
        final long retryWaitMs = 100L;

        Time mockTime = new MockTime(); // virtual clock; sleep() advances time but doesn’t block

        BigQuery bq = mockBigQuery(0, true);

        // storage succeeds.
        assertDoesNotThrow(() -> new GcsToBqWriter(mockStorage(null), bq, null,
                        retries, retryWaitMs, false, false, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "upload succeeded"
        );

        // storage throws retryable error
        String msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(true), mockBigQuery(0, true), mockSchemaManager(null, true),
                        retries, retryWaitMs, false, false, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "upload failed -- retry"
        ).getMessage();
        assertTrue(msg.startsWith("Timeout expired after"));

        // storage throws non-retryable error.
        msg = assertThrows(BigQueryConnectException.class, () -> new GcsToBqWriter(mockStorage(false), mockBigQuery(0, true), mockSchemaManager(null, true),
                        retries, retryWaitMs, false, false, mockTime).writeRows(oneRow(), tableId, "bucket", "blob"),
                "upload failed -- no retry"
        ).getMessage();
        assertEquals("Non-retryable exception on attempt 1.", msg);
    }

  @Nested
  @DisplayName("JSON serialization (Gson / ByteBuffer)")
  class JsonSerializationTests {
    @Test
    @DisplayName("Vanilla Gson fails on Debezium VariableScaleDecimal (Java 9+ only)")
    public void gsonFailsOnByteBuffer() {
      String specVersion = System.getProperty("java.specification.version", "8");
      assumeTrue(javaIsAtLeast9(specVersion), "Relevant only on Java 9+");

      byte[] raw = new byte[] {0x01, 0x23, (byte) 0xAB};
      Schema vsdSchema = VariableScaleDecimal.schema();
      Struct vsd = new Struct(vsdSchema).put("scale", 6).put("value", ByteBuffer.wrap(raw));

      Map<String, Object> record = new LinkedHashMap<>();
      record.put("id", 42);
      record.put("amount", vsd);

      Gson vanilla = new Gson();

      assertThrows(JsonIOException.class, () -> vanilla.toJson(record));
    }

    @Test
    @DisplayName("Safe Gson serializes VariableScaleDecimal as base64 (values[0]=scale, values[1]=bytes)")
    public void safeGsonPassesOnByteBuffer() {
      byte[] raw = new byte[]{0x01, 0x23, (byte) 0xAB};
      String expectedB64 = Base64.getEncoder().encodeToString(raw);

      Schema vsdSchema = VariableScaleDecimal.schema();
      Struct vsd = new Struct(vsdSchema)
             .put("scale", 6)
             .put("value", ByteBuffer.wrap(raw));

      Map<String, Object> record = new LinkedHashMap<>();
      record.put("id", 42);
      record.put("amount", vsd);

      String json = GsonUtils.SAFE_GSON.toJson(record);

      // Parse back and assert structure: amount.values = [6, "ASOr"]
      @SuppressWarnings("unchecked")
      Map<String, Object> parsed = new Gson().fromJson(json, Map.class);
      @SuppressWarnings("unchecked")
      Map<String, Object> amount = (Map<String, Object>) parsed.get("amount");
      @SuppressWarnings("unchecked")
      java.util.List<Object> values = (java.util.List<Object>) amount.get("values");

      // Gson parses numbers as Double
      assertEquals(6.0, values.get(0), "scale should be the first element in values[]");
      assertEquals(expectedB64, values.get(1), "encoded bytes should be the second element");
      }

    @Test
    @DisplayName("Safe Gson behaves like vanilla Gson for regular types")
    public void safeGsonPassesOnRegularTypes() {
      Map<String, Object> nativeOnly = new LinkedHashMap<>();
      nativeOnly.put("s", "str");
      nativeOnly.put("n", 123);
      nativeOnly.put("b", true);
      nativeOnly.put("list", Arrays.asList(1, 2, 3));

      Map<String, Object> nested = new LinkedHashMap<>();
      nested.put("x", 1);
      nativeOnly.put("obj", nested);

      Gson vanilla = new Gson();
      String vanillaJson = vanilla.toJson(nativeOnly);
      String safeJson = GsonUtils.SAFE_GSON.toJson(nativeOnly);

      assertEquals(
          vanillaJson,
          safeJson,
          "SAFE_GSON should behave exactly like vanilla Gson for native types");
    }
  }

  private void expectTable(BigQuery mockBigQuery) {
    Table mockTable = mock(Table.class);
    when(mockBigQuery.getTable(any(TableId.class))).thenReturn(mockTable);
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

  private static boolean javaIsAtLeast9(String specVersion) {
    if (specVersion.startsWith("1.")) return false; // Java 8 reports "1.8"
    try { return Integer.parseInt(specVersion.split("\\.")[0]) >= 9; }
    catch (Exception ignored) { return true; }
  }
}
