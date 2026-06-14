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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class StorageWriteApiWriterTest {
  Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
  Schema valueSchema = SchemaBuilder.struct()
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.STRING_SCHEMA)
          .field("available-name", Schema.BOOLEAN_SCHEMA)
          .field("bytes_check", Schema.BYTES_SCHEMA)
          .build();

  @Test
  public void testRecordConversion() {
    StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBase.class);
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    RecordConverter<Map<String, Object>> recordConverter = new BigQueryRecordConverter(
        false,
        true
    );
    when(mockedConfig.getRecordConverter()).thenReturn(recordConverter);
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);
    PartitionedTableId table = new PartitionedTableId.Builder(
            TableId.of("test-project", "scratch", "dummy_table")
    ).build();
    TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, table, sinkRecordConverter, batchModeHandler);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ConvertedRecord>> records = ArgumentCaptor.forClass(List.class);
    String expectedKafkaKey = "{\"key\":\"12345\"}";
    Set<String> expectedKeys = new HashSet<>();
    expectedKeys.add("id");
    expectedKeys.add("name");
    expectedKeys.add("available_name");
    expectedKeys.add("i_am_kafka_key");
    expectedKeys.add("i_am_kafka_record_detail");
    expectedKeys.add("bytes_check");

    Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.of("i_am_kafka_record_detail"));
    Mockito.when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
    Mockito.when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);

    builder.addRow(createRecord("abc", 100), null);
    builder.build().run();

    verify(mockStreamWriter, times(1))
        .initializeAndWriteRecords(any(PartitionedTableId.class), records.capture(), any());
    assertEquals(1, records.getValue().size());

    JSONObject actual = records.getValue().get(0).converted();
    assertEquals(expectedKeys, actual.keySet());

    String actualKafkaKey = actual.get("i_am_kafka_key").toString();
    assertEquals(expectedKafkaKey, actualKafkaKey);

    JSONObject recordDetails = (JSONObject) actual.get("i_am_kafka_record_detail");
    assertTrue(recordDetails.get("insertTime") instanceof Long);

    Object bytes_check = actual.get("bytes_check");
    assertTrue(bytes_check instanceof ByteString);
  }

  @Test
  public void testBatchLoadStreamName() {
    TableId fullTableId = TableId.of("p", "d", "t$20250722");
    TableId baseTableId = TableId.of("p", "d", "t");

    PartitionedTableId partitionedTableId = mock(PartitionedTableId.class);
    when(partitionedTableId.getFullTableId()).thenReturn(fullTableId);
    when(partitionedTableId.getBaseTableId()).thenReturn(baseTableId);

    TableName expectedTableName = TableNameUtils.tableName(baseTableId);
    String expectedStreamName = expectedTableName.toString() + "_s1";

    StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBatchApplicationStream.class);
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    RecordConverter<Map<String, Object>> recordConverter = new BigQueryRecordConverter(
        false, false);
    when (mockedConfig.getRecordConverter()).thenReturn(recordConverter);
    when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.empty());
    when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
    when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);

    // Capture table name passed into updateOffsetOnStream
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    ArgumentCaptor<String> tableNameCaptor = ArgumentCaptor.forClass(String.class);
    when(batchModeHandler.updateOffsetsOnStream(tableNameCaptor.capture(), any()))
            .thenReturn(expectedStreamName);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);
    TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
            mockStreamWriter, partitionedTableId, sinkRecordConverter, batchModeHandler);

    builder.addRow(createRecord("abc", 100), null);
    builder.build().run();

    // Capture stream name initializeAndWriteRecords was called with
    ArgumentCaptor<String> streamNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStreamWriter, times(1))
            .initializeAndWriteRecords(any(PartitionedTableId.class), any(), streamNameCaptor.capture());

    assertEquals(expectedStreamName, streamNameCaptor.getValue());
    String tableNameUsedInUpdate = tableNameCaptor.getValue();
    assertFalse(tableNameUsedInUpdate.contains("$"), "Partition decorator ($...) should not be used");
    assertEquals(expectedTableName.toString(), tableNameUsedInUpdate, "Base table name should be used for stream construction");
  }

  @Test
  public void testWriteAttemptIdRefreshedOnStorageApiInternalRetry() throws Exception {
    KafkaDataBuilder.setTrackPutAttempts(true);
    try {
      BigQuerySinkTaskConfig mockedConfig = buildTrackedConfig();
      SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);

      StorageWriteApiDefaultStream stream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
      JsonStreamWriter jsonWriter = mock(JsonStreamWriter.class);
      stream.tableToStream = new ConcurrentHashMap<>();
      stream.schemaManager = mock(SchemaManager.class);
      stream.errantRecordHandler = mock(ErrantRecordHandler.class);
      stream.time = new MockTime();
      doReturn(jsonWriter).when(stream).getDefaultStream(any(), any());
      doReturn(true).when(stream).canAttemptSchemaUpdate();

      AppendRowsResponse schemaUpdateResponse = AppendRowsResponse.newBuilder()
          .setUpdatedSchema(TableSchema.newBuilder().build())
          .build();
      AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
          .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType())
          .build();
      ApiFuture<AppendRowsResponse> future1 = mock(ApiFuture.class);
      when(future1.get()).thenReturn(schemaUpdateResponse);
      ApiFuture<AppendRowsResponse> future2 = mock(ApiFuture.class);
      when(future2.get()).thenReturn(successResponse);
      when(jsonWriter.append(any(JSONArray.class))).thenReturn(future1).thenReturn(future2);

      AtomicInteger counter = new AtomicInteger(0);
      Supplier<String> ulidSupplier = () -> "ULID-" + counter.incrementAndGet();

      PartitionedTableId table = new PartitionedTableId.Builder(
          TableId.of("test-project", "scratch", "dummy_table")).build();
      StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
      StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
          stream, table, sinkRecordConverter, batchModeHandler);
      builder.withUlidSupplier(ulidSupplier);
      builder.addRow(createRecord("abc", 100), null);
      builder.build().run();

      ArgumentCaptor<JSONArray> captor = ArgumentCaptor.forClass(JSONArray.class);
      verify(jsonWriter, times(2)).append(captor.capture());

      String idFirst = extractPutAttemptId(captor.getAllValues().get(0));
      String idRetry = extractPutAttemptId(captor.getAllValues().get(1));

      assertNotNull(idFirst, "First attempt should have a putAttemptId");
      assertNotNull(idRetry, "Retry attempt should have a putAttemptId");
      assertNotEquals(idFirst, idRetry,
          "Internal retry must produce a different putAttemptId than the first attempt");
    } finally {
      KafkaDataBuilder.setTrackPutAttempts(false);
    }
  }

  @Test
  public void testWriteAttemptIdNotSetWhenTrackingDisabledStorageApi() throws Exception {
    KafkaDataBuilder.setTrackPutAttempts(false);

    BigQuerySinkTaskConfig mockedConfig = buildTrackedConfig();
    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);

    StorageWriteApiDefaultStream stream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    JsonStreamWriter jsonWriter = mock(JsonStreamWriter.class);
    stream.tableToStream = new ConcurrentHashMap<>();
    stream.schemaManager = mock(SchemaManager.class);
    stream.errantRecordHandler = mock(ErrantRecordHandler.class);
    stream.time = new MockTime();
    doReturn(jsonWriter).when(stream).getDefaultStream(any(), any());
    doReturn(true).when(stream).canAttemptSchemaUpdate();

    AppendRowsResponse schemaUpdateResponse = AppendRowsResponse.newBuilder()
        .setUpdatedSchema(TableSchema.newBuilder().build())
        .build();
    AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
        .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType())
        .build();
    ApiFuture<AppendRowsResponse> future1 = mock(ApiFuture.class);
    when(future1.get()).thenReturn(schemaUpdateResponse);
    ApiFuture<AppendRowsResponse> future2 = mock(ApiFuture.class);
    when(future2.get()).thenReturn(successResponse);
    when(jsonWriter.append(any(JSONArray.class))).thenReturn(future1).thenReturn(future2);

    // No withUlidSupplier() call — tracking disabled
    PartitionedTableId table = new PartitionedTableId.Builder(
        TableId.of("test-project", "scratch", "dummy_table")).build();
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
        stream, table, sinkRecordConverter, batchModeHandler);
    builder.addRow(createRecord("abc", 100), null);
    builder.build().run();

    ArgumentCaptor<JSONArray> captor = ArgumentCaptor.forClass(JSONArray.class);
    verify(jsonWriter, times(2)).append(captor.capture());

    String idFirst = extractPutAttemptId(captor.getAllValues().get(0));
    String idRetry = extractPutAttemptId(captor.getAllValues().get(1));

    assertNull(idFirst, "putAttemptId should be absent when trackPutAttempts=false");
    assertNull(idRetry, "putAttemptId should be absent when trackPutAttempts=false on retry");
  }

  @Test
  public void testWriteAttemptIdPresentOnFirstAttemptStorageApi() throws Exception {
    KafkaDataBuilder.setTrackPutAttempts(true);
    try {
      BigQuerySinkTaskConfig mockedConfig = buildTrackedConfig();
      SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);

      StorageWriteApiDefaultStream stream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
      JsonStreamWriter jsonWriter = mock(JsonStreamWriter.class);
      stream.tableToStream = new ConcurrentHashMap<>();
      stream.schemaManager = mock(SchemaManager.class);
      stream.errantRecordHandler = mock(ErrantRecordHandler.class);
      stream.time = new MockTime();
      doReturn(jsonWriter).when(stream).getDefaultStream(any(), any());

      AppendRowsResponse successResponse = AppendRowsResponse.newBuilder()
          .setAppendResult(AppendRowsResponse.AppendResult.newBuilder().getDefaultInstanceForType())
          .build();
      ApiFuture<AppendRowsResponse> future = mock(ApiFuture.class);
      when(future.get()).thenReturn(successResponse);
      when(jsonWriter.append(any(JSONArray.class))).thenReturn(future);

      AtomicInteger counter = new AtomicInteger(0);
      Supplier<String> ulidSupplier = () -> "ULID-" + counter.incrementAndGet();

      PartitionedTableId table = new PartitionedTableId.Builder(
          TableId.of("test-project", "scratch", "dummy_table")).build();
      StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
      StorageWriteApiWriter.Builder builder = new StorageWriteApiWriter.Builder(
          stream, table, sinkRecordConverter, batchModeHandler);
      builder.withUlidSupplier(ulidSupplier);
      builder.addRow(createRecord("abc", 100), null);
      builder.build().run();

      ArgumentCaptor<JSONArray> captor = ArgumentCaptor.forClass(JSONArray.class);
      verify(jsonWriter, times(1)).append(captor.capture());

      String writeAttemptId = extractPutAttemptId(captor.getValue());
      assertNotNull(writeAttemptId, "First (and only) write attempt should carry a write-attempt ID");
    } finally {
      KafkaDataBuilder.setTrackPutAttempts(false);
    }
  }

  private BigQuerySinkTaskConfig buildTrackedConfig() {
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.of("_kafka_data"));
    when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.empty());
    when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(false);
    RecordConverter<Map<String, Object>> rc = new BigQueryRecordConverter(false, false);
    when(mockedConfig.getRecordConverter()).thenReturn(rc);
    return mockedConfig;
  }

  private String extractPutAttemptId(JSONArray records) {
    if (records.length() == 0) {
      return null;
    }
    JSONObject row = records.getJSONObject(0);
    if (!row.has("_kafka_data")) {
      return null;
    }
    JSONObject kafkaData = row.getJSONObject("_kafka_data");
    return kafkaData.optString("putAttemptId", null);
  }

  private SinkRecord createRecord(String topic, long offset) {
    Object key = new Struct(keySchema).put("key", "12345");
    Object value = new Struct(valueSchema)
        .put("id", 1L)
        .put("name", "1")
        .put("available-name", true)
        .put("bytes_check", new byte[]{47, 48, 49});
    return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
  }
}
