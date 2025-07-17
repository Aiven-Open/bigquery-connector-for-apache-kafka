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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;

public class StorageWriteApiDefaultStreamTest {

  private final PartitionedTableId mockedPartitionedTableId = new PartitionedTableId.Builder("dummyDataset", "dummyTable").setProject("dummyProject").build();
  private final JsonStreamWriter mockedStreamWriter = mock(JsonStreamWriter.class);
  private final SinkRecord mockedSinkRecord = new SinkRecord(
          "abc",
          0,
          Schema.BOOLEAN_SCHEMA,
          null,
          Schema.BOOLEAN_SCHEMA,
          null,
          0);
  private final ApiFuture<AppendRowsResponse> mockedResponse = mock(ApiFuture.class);
  private final List<ConvertedRecord> testRows = Collections.singletonList(new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final List<ConvertedRecord> testMultiRows = Arrays.asList(
          new ConvertedRecord(mockedSinkRecord, new JSONObject()),
          new ConvertedRecord(mockedSinkRecord, new JSONObject()));
  private final StorageWriteApiDefaultStream defaultStream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
  private final String nonRetriableExpectedException = "Failed to write rows on table "
          + TableNameUtils.tableName(mockedPartitionedTableId.getFullTableId()).toString()
          + " due to I am non-retriable error";
  private final String retriableExpectedException = "Exceeded 0 attempts to write to table "
          + TableNameUtils.tableName(mockedPartitionedTableId.getFullTableId()).toString() + " ";
  private final String malformedrequestExpectedException = "Insertion failed at table dummyTable for following rows:" +
          " \n [row index 0] (Failure reason : f0 field is unknown) ";
  ErrantRecordHandler mockedErrantRecordHandler = mock(ErrantRecordHandler.class);
  ErrantRecordReporter mockedErrantReporter = mock(ErrantRecordReporter.class);
  AppendRowsResponse malformedError = AppendRowsResponse.newBuilder()
          .setError(
                  Status.newBuilder()
                          .setCode(3)
                          .setMessage("I am an INVALID_ARGUMENT error")
                          .build()
          ).addRowErrors(
                  RowError.newBuilder()
                          .setIndex(0)
                          .setMessage("f0 field is unknown")
                          .build()
          ).build();
  AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
          .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType()).build();
  Map<Integer, String> errorMapping = new HashMap<>();
  Exceptions.AppendSerializtionError appendSerializationException = new Exceptions.AppendSerializtionError(
          3,
          "INVALID_ARGUMENT",
          "DEFAULT",
          errorMapping);
  AppendRowsResponse schemaError = AppendRowsResponse.newBuilder()
          .setUpdatedSchema(TableSchema.newBuilder().build())
          .build();
  ExecutionException tableMissingException = new ExecutionException(new StatusRuntimeException(
          io.grpc.Status
                  .fromCode(io.grpc.Status.Code.NOT_FOUND)
                  .withDescription("Not found: table. Table is deleted")
  ));
  SchemaManager mockedSchemaManager = mock(SchemaManager.class);
  MockTime time = new MockTime();

  @BeforeEach
  public void setUp() throws Exception {
    errorMapping.put(0, "f0 field is unknown");
    defaultStream.tableToStream = new ConcurrentHashMap<>();
    defaultStream.tableToStream.put("testTable", mockedStreamWriter);
    defaultStream.schemaManager = mockedSchemaManager;
    defaultStream.time = time;
    defaultStream.errantRecordHandler = mockedErrantRecordHandler;
    doReturn(mockedStreamWriter).when(defaultStream).getDefaultStream(any(), any());
    when(mockedStreamWriter.append(ArgumentMatchers.any())).thenReturn(mockedResponse);
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

  @Test
  public void testDefaultStreamNonRetriableError() throws Exception {
    AppendRowsResponse nonRetriableError = AppendRowsResponse.newBuilder()
            .setError(
                    Status.newBuilder()
                            .setCode(0)
                            .setMessage("I am non-retriable error")
                            .build()
            ).build();

    when(mockedResponse.get()).thenReturn(nonRetriableError);

    verifyException(nonRetriableExpectedException);
  }

  @Test
  public void testDefaultStreamRetriableError() throws Exception {
    AppendRowsResponse retriableError = AppendRowsResponse.newBuilder()
            .setError(
                    Status.newBuilder()
                            .setCode(0)
                            .setMessage("I am an INTERNAL error")
                            .build()
            ).build();

    when(mockedResponse.get()).thenReturn(retriableError);

    verifyException(retriableExpectedException);
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorAllToDLQ() throws Exception {
    when(mockedResponse.get()).thenReturn(malformedError);
    verifyDLQ(testRows);
  }

  @Test
  public void testDefaultStreamMalformedRequestErrorSomeToDLQ() throws Exception {
    when(mockedResponse.get()).thenReturn(malformedError).thenReturn(successResponse);
    assertThrows(
            BigQueryStorageWriteApiConnectException.class,
            () -> verifyDLQ(testMultiRows)
    );
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
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null)
    );
    verifyNoInteractions(mockedSchemaManager);
  }

  @Test
  public void testDefaultStreamNonRetriableException() throws Exception {
    InterruptedException exception = new InterruptedException("I am non-retriable error");

    when(mockedResponse.get()).thenThrow(exception);

    verifyException(nonRetriableExpectedException);
  }

  @Test
  public void testDefaultStreamRetriableException() throws Exception {
    ExecutionException exception = new ExecutionException(new StatusRuntimeException(
            io.grpc.Status.fromCode(io.grpc.Status.Code.INTERNAL).withDescription("I am an INTERNAL error")
    ));

    when(mockedResponse.get()).thenThrow(exception);

    verifyException(retriableExpectedException);
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionAllToDLQ() throws Exception {
    when(mockedResponse.get()).thenThrow(appendSerializationException);
    verifyDLQ(testRows);
  }

  @Test
  public void testDefaultStreamMalformedRequestExceptionSomeToDLQ() throws Exception {
    when(mockedResponse.get()).thenThrow(appendSerializationException).thenReturn(successResponse);
    assertThrows(
            BigQueryStorageWriteApiConnectException.class,
            () -> verifyDLQ(testMultiRows)
    );
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
    ExecutionException exception = new ExecutionException(
            new Throwable("Exceptions$StreamWriterClosedException due to FAILED_PRECONDITION"));
    when(mockedResponse.get()).thenThrow(exception);

    assertThrows(
            BigQueryStorageWriteApiConnectException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null)
    );
  }

  @Test
  public void testShutdown() {
    defaultStream.tableToStream = new ConcurrentHashMap<>();
    defaultStream.tableToStream.put("testTable", mockedStreamWriter);
    defaultStream.preShutdown();
    verify(mockedStreamWriter, times(1)).close();
  }

  private void verifyException(String expectedException) {
    BigQueryStorageWriteApiConnectException e = assertThrows(
            BigQueryStorageWriteApiConnectException.class,
            () -> defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, testRows, null)
    );
    assertEquals(expectedException, e.getMessage());
  }

  private void verifyDLQ(List<ConvertedRecord> rows) {
    ArgumentCaptor<Map<SinkRecord, Throwable>> captorRecord = ArgumentCaptor.forClass(Map.class);

    defaultStream.initializeAndWriteRecords(mockedPartitionedTableId, rows, null);

    verify(mockedErrantRecordHandler, times(1))
            .reportErrantRecords(captorRecord.capture());
    assertTrue(captorRecord.getValue().containsKey(mockedSinkRecord));
    assertEquals("f0 field is unknown", captorRecord.getValue().get(mockedSinkRecord).getMessage());
    assertEquals(1, captorRecord.getValue().size());
  }
}
