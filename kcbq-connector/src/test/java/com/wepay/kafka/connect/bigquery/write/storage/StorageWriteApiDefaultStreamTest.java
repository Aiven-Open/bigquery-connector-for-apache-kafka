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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

public class StorageWriteApiDefaultStreamTest {

  private final PartitionedTableId mockedPartitionedTableId =
      new PartitionedTableId.Builder("dummyDataset", "dummyTable")
          .setProject("dummyProject")
          .build();
  private final String mockedTableName =
      TableNameUtils.tableName(mockedPartitionedTableId.getFullTableId()).toString();
  private final JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
  private final SinkRecord mockedSinkRecord =
      new SinkRecord("abc", 0, Schema.BOOLEAN_SCHEMA, null, Schema.BOOLEAN_SCHEMA, null, 0);
  private final ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
  private final List<ConvertedRecord> testRows =
      Collections.singletonList(new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final List<ConvertedRecord> testMultiRows =
      Arrays.asList(
          new ConvertedRecord(mockedSinkRecord, new JSONObject()),
          new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final StorageWriteApiDefaultStream defaultStream =
      mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
  private final String baseErrorMessage = "Failed to write rows on table " + mockedTableName;
  private final String retriableExpectedException =
      "Exceeded 0 attempts to write to table " + mockedTableName + " ";
  private final String malformedrequestExpectedException =
      "Insertion failed at table dummyTable for following rows:"
          + " \n [row index 0] (Failure reason : f0 field is unknown) ";
  ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
  ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);
  AppendRowsResponse malformedError =
      AppendRowsResponse.newBuilder()
          .setError(
              Status.newBuilder().setCode(3).setMessage("I am an INVALID_ARGUMENT error").build())
          .addRowErrors(RowError.newBuilder().setIndex(0).setMessage("f0 field is unknown").build())
          .build();
  AppendRowsResponse successResponse =
      AppendRowsResponse.newBuilder()
          .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType())
          .build();
  Map<Integer, String> errorMapping = new HashMap<>();
  Exceptions.AppendSerializtionError appendSerializationException =
      new Exceptions.AppendSerializtionError(3, "INVALID_ARGUMENT", "DEFAULT", errorMapping);
  AppendRowsResponse schemaError =
      AppendRowsResponse.newBuilder().setUpdatedSchema(TableSchema.newBuilder().build()).build();
  ExecutionException tableMissingException =
      new ExecutionException(
          new StatusRuntimeException(
              io.grpc.Status.fromCode(io.grpc.Status.Code.NOT_FOUND)
                  .withDescription("Not found: table. Table is deleted")));
  SchemaManager mockedSchemaManager = mock(SchemaManager.class);
  MockTime time = new MockTime();

  @BeforeEach
  public void setUp() throws Exception {
    errorMapping.put(0, "f0 field is unknown");
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.tableToStreams.put(
        "testTable", new AtomicReferenceArray<>(new JsonStreamWriter[] {mockedStreamWriter}));
    defaultStream.writersPerTable = 1;
    defaultStream.threadWriterSlot = ThreadLocal.withInitial(() -> 0);
    defaultStream.schemaManager = mockedSchemaManager;
    defaultStream.time = time;
    defaultStream.errantRecordHandler = mockedErrantRecordHandler;
    doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(any(), any());
    when(mockedStreamWriter.append(ArgumentMatchers.any(JSONArray.class)))
        .thenReturn(mockedResponse);
    doReturn(true).when(mockedSchemaManager).createTable(any(), any());
    doNothing().when(mockedSchemaManager).updateSchema(any(), any());
    when(mockedErrantRecordHandler.getErrantRecordReporter()).thenReturn(mockedErrantReporter);
    when(defaultStream.getAutoCreateTables()).thenReturn(true);
    when(defaultStream.canAttemptSchemaUpdate()).thenReturn(true);
  }

  @Test
  public void testDefaultStreamNoExceptions() throws Exception {
    when(mockedResponse.get()).thenReturn(successResponse);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null);
  }

  @ParameterizedTest(name = "{index} – {0}")
  @MethodSource("terminalClientErrors")
  public void testDefaultStreamTerminalClientErrors(String testCase, String errorMessage)
      throws Exception {
    AppendRowsResponse clientError =
        AppendRowsResponse.newBuilder()
            .setError(Status.newBuilder().setCode(0).setMessage(errorMessage).build())
            .build();

    when(mockedResponse.get()).thenReturn(clientError);

    verifyTerminalException(errorMessage);
  }

  public static Stream<Arguments> terminalClientErrors() {
    return Stream.of(
        Arguments.of("Non-retriable errors", "I am non-retriable error"),
        Arguments.of("Request-level errors", "I am an INTERNAL error"));
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorAllToDLQ() throws Exception {
    when(mockedResponse.get()).thenReturn(malformedError);
    verifyDLQ(testRows);
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorSomeToDLQ() throws Exception {
    when(mockedResponse.get()).thenReturn(malformedError).thenReturn(successResponse);
    assertThrows(BigQueryStorageWriteApiConnectException.class, () -> verifyDLQ(testMultiRows));
  }

  @Test
  public void testHasSchemaUpdates() throws Exception {
    when(mockedResponse.get()).thenReturn(schemaError).thenReturn(successResponse);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null);

    verify(mockedSchemaManager, times(1)).updateSchema(any(), any());
  }

  @Test
  public void testHasSchemaUpdatesNotConfigured() throws Exception {
    when(mockedResponse.get()).thenReturn(schemaError).thenReturn(successResponse);
    when(defaultStream.canAttemptSchemaUpdate()).thenReturn(false);

    assertThrows(
        BigQueryStorageWriteApiConnectException.class,
        () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null));
    verifyNoInteractions(mockedSchemaManager);
  }

  @ParameterizedTest(name = "{index} – {0}")
  @MethodSource("terminalClientExceptions")
  public void testDefaultStreamTerminalClientException(String testCase, Exception exception)
      throws Exception {
    when(mockedResponse.get()).thenThrow(exception);

    verifyTerminalException(exception.getMessage());
  }

  public static Stream<Arguments> terminalClientExceptions() {
    return Stream.of(
        Arguments.of("Non-retriable errors", new InterruptedException("I am non-retriable error")),
        Arguments.of(
            "Request-level errors",
            new ExecutionException(
                new StatusRuntimeException(
                    io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL)
                        .withDescription("I am an INTERNAL error")))));
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionAllToDLQ() throws Exception {
    when(mockedResponse.get()).thenThrow(appendSerializationException);
    verifyDLQ(testRows);
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionSomeToDLQ() throws Exception {
    when(mockedResponse.get()).thenThrow(appendSerializationException).thenReturn(successResponse);
    assertThrows(BigQueryStorageWriteApiConnectException.class, () -> verifyDLQ(testMultiRows));
  }

  @Test
  public void testDefaultStreamTableMissingException() throws Exception {
    when(mockedResponse.get()).thenThrow(tableMissingException).thenReturn(successResponse);
    when(defaultStream.getAutoCreateTables()).thenReturn(true);
    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null);
    verify(mockedSchemaManager, times(1)).createTable(any(), any());
  }

  @Test
  public void testHasSchemaUpdatesException() throws Exception {
    errorMapping.put(0, "JSONObject does not have the required field f1");
    when(mockedResponse.get()).thenThrow(appendSerializationException).thenReturn(successResponse);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null);
    verify(mockedSchemaManager, times(1)).updateSchema(any(), any());
  }

  @Test
  public void testDefaultStreamClosedException() throws Exception {
    ExecutionException exception =
        new ExecutionException(
            new Throwable("Exceptions$StreamWriterClosedException due to FAILED_PRECONDITION"));
    when(mockedResponse.get()).thenThrow(exception);

    assertThrows(
        BigQueryStorageWriteApiConnectException.class,
        () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null));
  }

  @Test
  public void testShutdown() {
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.tableToStreams.put(
        "testTable", new AtomicReferenceArray<>(new JsonStreamWriter[] {mockedStreamWriter}));
    defaultStream.preShutdown();
    verify(mockedStreamWriter, times(1)).close();
  }

  @Test
  public void testGetDefaultStreamCreatesOneWriterPerSlotLazily() {
    JsonStreamWriter w0 = mock(JsonStreamWriter.class);
    JsonStreamWriter w1 = mock(JsonStreamWriter.class);
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.writersPerTable = 2;
    defaultStream.threadWriterSlot = ThreadLocal.withInitial(() -> 0);
    doCallRealMethod().when(defaultStream).getDefaultStream(any(), any());
    doReturn(w0, w1).when(defaultStream).createDefaultStream(any(), any(), any());

    assertSame(w0, defaultStream.getDefaultStream(mockedPartitionedTableId, testRows));
    assertSame(w0, defaultStream.getDefaultStream(mockedPartitionedTableId, testRows));
    assertNull(defaultStream.tableToStreams.get(mockedTableName).get(1));

    defaultStream.threadWriterSlot.set(1);
    assertSame(w1, defaultStream.getDefaultStream(mockedPartitionedTableId, testRows));
    verify(defaultStream, times(2)).createDefaultStream(any(), any(), any());
  }

  @Test
  public void testSlotAssignerIsRoundRobinAcrossThreadsAndStickyPerThread() throws Exception {
    ThreadLocal<Integer> slots = StorageWriteApiDefaultStream.slotAssigner(2);
    List<Integer> firstCalls = new CopyOnWriteArrayList<>();
    List<Boolean> sticky = new CopyOnWriteArrayList<>();
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Thread t =
          new Thread(
              () -> {
                int slot = slots.get();
                firstCalls.add(slot);
                sticky.add(slot == slots.get());
              });
      threads.add(t);
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    assertEquals(2, Collections.frequency(firstCalls, 0));
    assertEquals(2, Collections.frequency(firstCalls, 1));
    assertTrue(sticky.stream().allMatch(Boolean::booleanValue));
  }

  @Test
  public void testRefreshClosesOnlyTheFailedWriterSlot() throws Exception {
    JsonStreamWriter other = mock(JsonStreamWriter.class);
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.tableToStreams.put(
        mockedTableName,
        new AtomicReferenceArray<>(new JsonStreamWriter[] {mockedStreamWriter, other}));
    defaultStream.threadWriterSlot = ThreadLocal.withInitial(() -> 0);
    StorageWriteApiDefaultStream.DefaultStreamWriter writer =
        defaultStream.new DefaultStreamWriter(mockedPartitionedTableId, testRows);
    writer.appendRows(new JSONArray());

    writer.refresh();

    verify(mockedStreamWriter, times(1)).close();
    verify(other, times(0)).close();
    AtomicReferenceArray<JsonStreamWriter> writers =
        defaultStream.tableToStreams.get(mockedTableName);
    assertNull(writers.get(0));
    assertSame(other, writers.get(1));
  }

  @Test
  public void testRefreshLeavesAlreadyReplacedWriterAlone() throws Exception {
    JsonStreamWriter replacement = mock(JsonStreamWriter.class);
    AtomicReferenceArray<JsonStreamWriter> writers =
        new AtomicReferenceArray<>(new JsonStreamWriter[] {mockedStreamWriter});
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.tableToStreams.put(mockedTableName, writers);
    defaultStream.threadWriterSlot = ThreadLocal.withInitial(() -> 0);
    StorageWriteApiDefaultStream.DefaultStreamWriter writer =
        defaultStream.new DefaultStreamWriter(mockedPartitionedTableId, testRows);
    writer.appendRows(new JSONArray());
    writers.set(0, replacement);

    writer.refresh();

    verify(mockedStreamWriter, never()).close();
    verify(replacement, never()).close();
    assertSame(replacement, writers.get(0));
  }

  @Test
  public void testShutdownClosesAllWritersPerTable() {
    JsonStreamWriter writerA = mock(JsonStreamWriter.class);
    JsonStreamWriter writerB = mock(JsonStreamWriter.class);
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    // slot 1 never opened a stream
    defaultStream.tableToStreams.put(
        "testTable", new AtomicReferenceArray<>(new JsonStreamWriter[] {writerA, null, writerB}));
    defaultStream.preShutdown();
    verify(writerA, times(1)).close();
    verify(writerB, times(1)).close();
    assertFalse(defaultStream.tableToStreams.containsKey("testTable"));
  }

  @Test
  public void testShutdownToleratesWriterCloseFailure() {
    JsonStreamWriter failing = mock(JsonStreamWriter.class);
    JsonStreamWriter ok = mock(JsonStreamWriter.class);
    doThrow(new RuntimeException("close failed")).when(failing).close();
    defaultStream.tableToStreams = new ConcurrentHashMap<>();
    defaultStream.tableToStreams.put(
        "testTable", new AtomicReferenceArray<>(new JsonStreamWriter[] {failing, ok}));
    defaultStream.preShutdown();
    verify(failing, times(1)).close();
    verify(ok, times(1)).close();
    assertFalse(defaultStream.tableToStreams.containsKey("testTable"));
  }

  private void verifyTerminalException(String expectedException) {
    BigQueryStorageWriteApiConnectException e =
        assertThrows(
            BigQueryStorageWriteApiConnectException.class,
            () ->
                defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null));

    assertAll(
        () ->
            assertTrue(
                e.getMessage().startsWith(baseErrorMessage), "Should fail task with base message"),
        () ->
            assertTrue(
                e.getMessage().contains(expectedException), "Should include cause of failure"),
        () ->
            assertFalse(
                e.getMessage().contains(retriableExpectedException),
                "Should not use connector retry path"));
  }

  private void verifyDLQ(List<ConvertedRecord> rows) {
    ArgumentCaptor<Map<SinkRecord, Throwable>> captorRecord = ArgumentCaptor.forClass(Map.class);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, rows, null);

    verify(mockedErrantRecordHandler, times(1)).reportErrantRecords(captorRecord.capture());
    assertTrue(captorRecord.getValue().containsKey(mockedSinkRecord));
    assertEquals("f0 field is unknown", captorRecord.getValue().get(mockedSinkRecord).getMessage());
    assertEquals(1, captorRecord.getValue().size());
  }
}
