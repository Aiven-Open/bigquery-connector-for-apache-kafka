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

package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BigQuerySinkTaskTest {
  private static SinkTaskPropertiesFactory propertiesFactory;

  private static AtomicLong spoofedRecordOffset = new AtomicLong();

  private static StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream = mock(StorageWriteApiDefaultStream.class);
  private static StorageApiBatchModeHandler mockedBatchHandler = mock(StorageApiBatchModeHandler.class);

  private final Time time = new MockTime();

  @BeforeAll
  public static void initializePropertiesFactory() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   *
   * @param topic         The topic of the record.
   * @param keyField      The field name for the record key; may be null.
   * @param key           The content of the record key; may be null.
   * @param valueField    The field name for the record value; may be null
   * @param value         The content of the record value; may be null
   * @param timestampType The type of timestamp embedded in the message
   * @param timestamp     The timestamp in milliseconds
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String keyField, String key,
                                           String valueField, String value,
                                           TimestampType timestampType, Long timestamp) {
    Schema basicKeySchema = null;
    Struct basicKey = null;
    if (keyField != null) {
      basicKeySchema = SchemaBuilder
          .struct()
          .field(keyField, Schema.STRING_SCHEMA)
          .build();
      basicKey = new Struct(basicKeySchema);
      basicKey.put(keyField, key);
    }

    Schema basicValueSchema = null;
    Struct basicValue = null;
    if (valueField != null) {
      basicValueSchema = SchemaBuilder
          .struct()
          .field(valueField, Schema.STRING_SCHEMA)
          .build();
      basicValue = new Struct(basicValueSchema);
      basicValue.put(valueField, value);
    }

    return new SinkRecord(topic, 0, basicKeySchema, basicKey,
        basicValueSchema, basicValue, spoofedRecordOffset.getAndIncrement(), timestamp, timestampType);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   *
   * @param topic         The topic of the record.
   * @param field         The field name for the record value.
   * @param value         The content of the record value.
   * @param timestampType The type of timestamp embedded in the message
   * @param timestamp     The timestamp in milliseconds
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String field, String value,
                                           TimestampType timestampType, Long timestamp) {
    return spoofSinkRecord(topic, null, null, field, value, timestampType, timestamp);
  }

  /**
   * Initialize the converters.  This is normally done by BigQuerySinkConnector before task is created.
   * @param properties the configuration properties.
   */
  private void initialize(Map<String, String> properties) {
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);
    DebeziumLogicalConverters.initialize(config);
    KafkaLogicalConverters.initialize(config);
  }

  @Test
  public void testGetRecordTableUsesConfiguredProject() throws Exception {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.USE_CREDENTIALS_PROJECT_ID_CONFIG, "false");
    initialize(properties);

    BigQuerySinkTask task = new BigQuerySinkTask(
        mock(BigQuery.class),
        mock(SchemaRetriever.class),
        mock(Storage.class),
        mock(SchemaManager.class),
        new HashMap<>(),
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    task.initialize(mock(SinkTaskContext.class));
    task.start(properties);

    SinkRecord record = spoofSinkRecord("kcbq-test", "field", "value",
        TimestampType.NO_TIMESTAMP_TYPE, null);
    java.lang.reflect.Method m = BigQuerySinkTask.class
        .getDeclaredMethod("getRecordTable", SinkRecord.class);
    m.setAccessible(true);
    PartitionedTableId tableId = (PartitionedTableId) m.invoke(task, record);

    assertEquals("test-project", tableId.getProject());
  }

  @Test
  public void testGetRecordTableUsesCredentialsProject() throws Exception {
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.USE_CREDENTIALS_PROJECT_ID_CONFIG, "true");
    initialize(properties);

    BigQuerySinkTask task = new BigQuerySinkTask(
        mock(BigQuery.class),
        mock(SchemaRetriever.class),
        mock(Storage.class),
        mock(SchemaManager.class),
        new HashMap<>(),
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );
    task.initialize(mock(SinkTaskContext.class));
    task.start(properties);

    SinkRecord record = spoofSinkRecord("kcbq-test", "field", "value",
        TimestampType.NO_TIMESTAMP_TYPE, null);
    java.lang.reflect.Method m = BigQuerySinkTask.class
        .getDeclaredMethod("getRecordTable", SinkRecord.class);
    m.setAccessible(true);
    PartitionedTableId tableId = (PartitionedTableId) m.invoke(task, record);
    assertNull(tableId.getProject());
  }


  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put()
   *
   * @param topic       The topic of the record.
   * @param valueSchema The schema of the record.
   * @param value       The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, Schema valueSchema, Struct value) {
    return new SinkRecord(topic, 0, null, null, valueSchema, value, 0);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a given name and value.
   *
   * @param topic The topic of the record.
   * @param field The name of the field in the record's struct.
   * @param value The content of the field.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String field, String value) {
    return spoofSinkRecord(topic, field, value, TimestampType.NO_TIMESTAMP_TYPE, null);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a given
   * value.
   *
   * @param topic The topic of the record.
   * @param value The content of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic, String value) {
    return spoofSinkRecord(topic, "sink_task_test_field", value);
  }

  /**
   * Utility method for spoofing SinkRecords that should be passed to SinkTask.put(). Creates a
   * record with a struct schema that contains only one string field with a default name and a
   * default value.
   *
   * @param topic The topic of the record.
   * @return The spoofed SinkRecord.
   */
  public static SinkRecord spoofSinkRecord(String topic) {
    return spoofSinkRecord(topic, "sink task test row");
  }

  @BeforeEach
  public void setUp() {
    MergeBatches.setStreamingBufferAvailabilityWait(0);
    spoofedRecordOffset.set(0);
  }

  @AfterEach
  public void cleanUp() {
    MergeBatches.resetStreamingBufferAvailabilityWait();
  }

  @Test
  public void testSimplePut() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testPutForGCSToBQ() {
    final String topic = "test-topic";
    final int repeats = 20;
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, "test-topic");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    IntStream.range(0, repeats).forEach(i -> testTask.put(Collections.singletonList(spoofSinkRecord(topic))));

    ArgumentCaptor<BlobInfo> blobInfo = ArgumentCaptor.forClass(BlobInfo.class);
    testTask.flush(Collections.emptyMap());

    verify(storage, times(repeats)).create(blobInfo.capture(), (byte[]) anyObject());
    assertEquals(repeats, blobInfo.getAllValues().stream().map(info -> info.getBlobId().getName()).collect(Collectors.toSet()).size());
  }

  @Test
  public void testSimplePutWhenSchemaRetrieverIsNotNull() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    SinkRecord spoofedRecord =
        spoofSinkRecord(topic, "k", "key", "v", "value", TimestampType.NO_TIMESTAMP_TYPE, null);
    testTask.put(Collections.singletonList(spoofedRecord));
    testTask.flush(Collections.emptyMap());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));
  }

  @Test
  public void testEmptyPut() {
    Map<String, String> properties = propertiesFactory.getProperties();
    initialize(properties);
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

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
    testTask.start(properties);

    testTask.put(Collections.emptyList());
  }

  // needed because debezium sends null messages when deleting messages in kafka
  @Test
  public void testEmptyRecordPut() {
    final String topic = "test_topic";
    final Schema simpleSchema = SchemaBuilder
        .struct()
        .field("aField", Schema.STRING_SCHEMA)
        .build();

    Map<String, String> properties = propertiesFactory.getProperties();
    initialize(properties);
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

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

    testTask.start(properties);

    SinkRecord emptyRecord = spoofSinkRecord(topic, simpleSchema, null);

    testTask.put(Collections.singletonList(emptyRecord));
  }

  @Test
  public void testPutWhenPartitioningOnMessageTime() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);

    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic$20171026", argument.getValue().getTable().getTable());
  }

  @Test
  public void testPutWhenPartitioningIsSetToTrue() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    properties.put(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);

    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic$20171026", argument.getValue().getTable().getTable());
  }

  @Test
  public void testPutWhenPartitioningIsSetToFalse() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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
    testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
        TimestampType.CREATE_TIME, 1509007584334L)));
    testTask.flush(Collections.emptyMap());
    ArgumentCaptor<InsertAllRequest> argument = ArgumentCaptor.forClass(InsertAllRequest.class);

    verify(bigQuery, times(1)).insertAll(argument.capture());
    assertEquals("test-topic", argument.getValue().getTable().getTable());
  }

  // Make sure a connect exception is thrown when the message has no timestamp type
  @Test
  public void testPutWhenPartitioningOnMessageTimeWhenNoTimestampType() {
    final String topic = "test-topic";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    properties.put(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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

    assertThrows(
        ConnectException.class,
        () -> testTask.put(Collections.singletonList(spoofSinkRecord(topic, "value", "message text",
          TimestampType.NO_TIMESTAMP_TYPE, null)))
    );
  }

  @Test
  public void testPutWithUpsertDelete() throws Exception {
    final String topic = "test-topic";
    final String key = "kafkaKey";
    final String value = "recordValue";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    properties.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");
    properties.put(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, "-1");
    properties.put(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG, "2");
    properties.put(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG, key);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();
    Field keyField = Field.of(key, LegacySQLTypeName.STRING);
    Field valueField = Field.of(value, LegacySQLTypeName.STRING);
    com.google.cloud.bigquery.Schema intermediateSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_BATCH_NUMBER_FIELD, LegacySQLTypeName.INTEGER)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
            .setMode(Field.Mode.NULLABLE)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, LegacySQLTypeName.RECORD, keyField)
            .setMode(Field.Mode.REQUIRED)
            .build(),
        Field.newBuilder(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, LegacySQLTypeName.RECORD, valueField)
            .build()
    );
    when(schemaManager.cachedSchema(any())).thenReturn(intermediateSchema);

    CountDownLatch executedMerges = new CountDownLatch(2);
    CountDownLatch executedBatchClears = new CountDownLatch(2);

    when(bigQuery.query(any(QueryJobConfiguration.class))).then(invocation -> {
      String query = invocation.getArgument(0, QueryJobConfiguration.class).getQuery();
      if (query.startsWith("MERGE")) {
        executedMerges.countDown();
      } else if (query.startsWith("DELETE")) {
        executedBatchClears.countDown();
      }
      return null;
    });

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

    // Insert a few regular records and one tombstone record
    testTask.put(Arrays.asList(
        spoofSinkRecord(topic, key, "4761", "value", "message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "489", "value", "other message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "28980", "value", "more message text", TimestampType.NO_TIMESTAMP_TYPE, null),
        spoofSinkRecord(topic, key, "4761", null, null, TimestampType.NO_TIMESTAMP_TYPE, null)
    ));

    assertTrue(
        executedMerges.await(5, TimeUnit.SECONDS),
        "Merge queries should be executed"
    );
    assertTrue(
        executedBatchClears.await(1, TimeUnit.SECONDS),
        "Batch clears should be executed"
    );
  }

  // Throw an exception on the first put, and assert the Exception will be exposed in subsequent
  // put call.
  @Test
  public void testSimplePutException() throws InterruptedException {
    final String topic = "test-topic";
    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    String error = "Cannot add required fields to an existing schema.";
    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    when(bigQuery.insertAll(any()))
        .thenThrow(
            new BigQueryException(400, error, new BigQueryError("invalid", "global", error)));

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

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    BigQueryConnectException e = assertThrows(
        BigQueryConnectException.class,
        () -> {
          // Try for at most 30 seconds to get the task to throw an error
          for (long startTime = System.currentTimeMillis(); System.currentTimeMillis() < startTime + 30_000; Thread.sleep(100)) {
            testTask.put(Collections.emptyList());
          }
        }
    );
    assertTrue(e.getCause().getCause().getMessage().contains(error));
  }

  @Test
  public void testEmptyFlush() {
    Map<String, String> properties = propertiesFactory.getProperties();
    initialize(properties);
    BigQuery bigQuery = mock(BigQuery.class);
    Storage storage = mock(Storage.class);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
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

    testTask.flush(Collections.emptyMap());
  }

  @Test
  public void testFlushAfterStop() {
    Map<String, String> properties = propertiesFactory.getProperties();
    initialize(properties);
    Storage storage = mock(Storage.class);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.insertAll(any()))
        .thenThrow(
            new BigQueryException(400, "Oops", new BigQueryError("invalid", "global", "oops")));

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
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

    testTask.put(Collections.singletonList(spoofSinkRecord("t")));
    assertThrows(
        Exception.class,
        () -> testTask.flush(Collections.emptyMap()),
        "first call to flush should fail"
    );
    assertThrows(
        Exception.class,
        () -> testTask.flush(Collections.emptyMap()),
        "second call to flush should fail"
    );
    testTask.stop();
    assertThrows(
        Exception.class,
        () -> testTask.flush(Collections.emptyMap()),
        "third call to flush (after task stop) should fail"
    );
  }

  @Test
  public void testBigQueryReadTimeout() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "3");
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any())).thenThrow(new BigQueryException(new SocketTimeoutException("mock timeout")));

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
    assertThrows(
        RetriableException.class,
        () -> testTask.put(Collections.singletonList(spoofSinkRecord(topic)))
    );
  }

  @Test
  public void testBigQuery5XXRetry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "3");
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(500, "mock 500"))
        .thenThrow(new BigQueryException(502, "mock 502"))
        .thenThrow(new BigQueryException(503, "mock 503"))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(4)).insertAll(anyObject());
  }

  @Test
  public void testBigQuery403Retry() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "2");
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    BigQueryError rateLimitExceededError = new BigQueryError("rateLimitExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError))
        .thenThrow(new BigQueryException(403, "mock rate limit exceeded", rateLimitExceededError))
        .thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

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
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    verify(bigQuery, times(3)).insertAll(anyObject());
  }

  @Test
  public void testBigQueryRetryExceeded() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "1");
    properties.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "2000");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);

    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);
    BigQueryError quotaExceededError = new BigQueryError("quotaExceeded", null, null);
    when(bigQuery.insertAll(anyObject()))
        .thenThrow(new BigQueryException(403, "mock quota exceeded", quotaExceededError));
    when(insertAllResponse.hasErrors()).thenReturn(false);

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
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    assertThrows(
        BigQueryConnectException.class,
        () -> testTask.flush(Collections.emptyMap())
    );
  }

  // Make sure that an InterruptedException is properly translated into a ConnectException
  @Test
  public void testInterruptedException() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    Storage storage = mock(Storage.class);
    InsertAllResponse fakeResponse = mock(InsertAllResponse.class);
    when(fakeResponse.hasErrors()).thenReturn(false);
    when(fakeResponse.getInsertErrors()).thenReturn(Collections.emptyMap());
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(fakeResponse);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
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

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    testTask.flush(Collections.emptyMap());

    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));
    Thread.currentThread().interrupt();
    assertThrows(
        ConnectException.class,
        () -> testTask.flush(Collections.emptyMap())
    );
  }

  @Test
  public void testTimePartitioningIncompatibleWithDecoratorSyntax() {
    final String topic = "t1";
    final String dataset = "d";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "true");
    properties.put(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, "true");
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    StandardTableDefinition mockTableDefinition = mock(StandardTableDefinition.class);
    when(mockTableDefinition.getTimePartitioning()).thenReturn(TimePartitioning.of(TimePartitioning.Type.HOUR));
    Table table = mock(Table.class);
    when(table.getDefinition()).thenReturn(mockTableDefinition);
    Map<TableId, Table> tableCache = new HashMap<>();
    tableCache.put(
        TableId.of(properties.get(BigQuerySinkConfig.PROJECT_CONFIG), dataset, topic),
        table
    );
    Storage storage = mock(Storage.class);
    BigQuery bigQuery = mock(BigQuery.class);

    BigQuerySinkTask testTask = new BigQuerySinkTask(
        bigQuery,
        null,
        storage,
        null,
        tableCache,
        mockedStorageWriteApiDefaultStream,
        mockedBatchHandler,
        time
    );

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    testTask.initialize(sinkTaskContext);
    testTask.start(properties);

    assertThrows(
        ConnectException.class,
        () -> testTask.put(Collections.singleton(spoofSinkRecord(topic, "f1", "v1", TimestampType.CREATE_TIME, 1L)))
    );
  }

  @Test
  public void testVersion() {
    assertNotNull(new BigQuerySinkTask().version());
  }

  // Existing tasks should succeed upon stop is called. New tasks should be rejected once task is
  // stopped.
  @Test
  public void testStop() {
    final String topic = "test_topic";
    final String dataset = "scratch";

    Map<String, String> properties = propertiesFactory.getProperties();
    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset);
    initialize(properties);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any())).thenReturn(mockTable);

    SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
    InsertAllResponse insertAllResponse = mock(InsertAllResponse.class);

    when(bigQuery.insertAll(anyObject())).thenReturn(insertAllResponse);
    when(insertAllResponse.hasErrors()).thenReturn(false);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    Map<TableId, Table> cache = new HashMap<>();

    Storage storage = mock(Storage.class);
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
    testTask.put(Collections.singletonList(spoofSinkRecord(topic)));

    assertEquals(1, testTask.getTaskThreadsActiveCount());
    testTask.stop();
    assertEquals(0, testTask.getTaskThreadsActiveCount());
    verify(bigQuery, times(1)).insertAll(any(InsertAllRequest.class));

    assertThrows(
        RejectedExecutionException.class,
        () -> testTask.put(Collections.singletonList(spoofSinkRecord(topic)))
    );
  }
}
