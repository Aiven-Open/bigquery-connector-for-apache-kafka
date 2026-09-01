/*
 * Copyright 2026 Aiven Oy and
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

import static com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter.CDC_CHANGE_SEQUENCE_NUMBER_FIELD;
import static com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter.CDC_CHANGE_TYPE_DELETE;
import static com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter.CDC_CHANGE_TYPE_FIELD;
import static com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter.CDC_CHANGE_TYPE_UPSERT;
import static com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter.DELETED_PSEUDO_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import de.huxhorn.sulky.ulid.ULID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SinkRecordConverterTest {
  private static final String TOPIC = "test-topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 42L;

  private Schema keySchema;
  private Schema valueSchema;
  private Struct keyStruct;
  private Struct valueStruct;

  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;

  private static final String kafkaDataTopicValue = "testTopic";
  private static final int kafkaDataPartitionValue = 101;
  private static final long kafkaDataOffsetValue = 1337;
  private static final String kafkaDataMutatedTopicValue = "mutatedTopic";
  private static final int kafkaDataMutatedPartitionValue = 201;
  private static final long kafkaDataMutatedOffsetValue = 456;
  private static final ULID ulid = new ULID();

  @BeforeEach
  public void setUp() {
    keySchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
    keyStruct = new Struct(keySchema).put("id", 123L);

    valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build();
    valueStruct = new Struct(valueSchema).put("id", 123L).put("name", "Alice");

    config = mock(BigQuerySinkTaskConfig.class);
    recordConverter = new BigQueryRecordConverter(false, true);

    when(config.getRecordConverter()).thenReturn(recordConverter);
    when(config.getLong(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG)).thenReturn(-1L);
    when(config.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG))
        .thenReturn(false);
    when(config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG))
        .thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(false);
    when(config.getKafkaKeyFieldName()).thenReturn(Optional.empty());
    when(config.getKafkaDataFieldName()).thenReturn(Optional.empty());
    when(config.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
  }

  @Test
  public void testCdcRowUpsert() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record =
        new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    // Verify root fields
    assertEquals(123L, actual.get("id"));
    assertEquals("Alice", actual.get("name"));

    // Verify CDC metadata columns
    assertEquals(CDC_CHANGE_TYPE_UPSERT, actual.get(CDC_CHANGE_TYPE_FIELD));
    assertEquals("00000001/000000000000002A", actual.get(CDC_CHANGE_SEQUENCE_NUMBER_FIELD));
  }

  @Test
  public void testCdcRowDelete() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, null, null, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    // Verify root fields from key
    assertEquals(123L, actual.get("id"));
    // Since value is null, "name" should not exist or should be null
    assertTrue(!actual.containsKey("name") || actual.get("name") == null);

    // Verify CDC metadata columns
    assertEquals(CDC_CHANGE_TYPE_DELETE, actual.get(CDC_CHANGE_TYPE_FIELD));
    assertEquals("00000001/000000000000002A", actual.get(CDC_CHANGE_SEQUENCE_NUMBER_FIELD));
  }

  @Test
  public void testCdcRowDeleteThrowsIfKeyNull() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record =
        new SinkRecord(TOPIC, PARTITION, null, null, valueSchema, valueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    assertThrows(ConnectException.class, () -> sinkRecordConverter.getCdcRow(record));
  }

  @Test
  public void testCdcRowWithCustomSeqFromValue() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getCdcChangeSequenceNumberField()).thenReturn(Optional.of("name"));

    SinkRecord record =
        new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals("UPSERT", actual.get("_CHANGE_TYPE"));
    assertEquals("416C696365/00000001/000000000000002A", actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowWithCustomSeqFromKey() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getCdcChangeSequenceNumberField()).thenReturn(Optional.of("id"));

    SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, null, null, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals("DELETE", actual.get("_CHANGE_TYPE"));
    assertEquals(
        "000000000000007B/00000001/000000000000002A", actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowWithKafkaTimestampSeq() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getCdcChangeSequenceNumberField()).thenReturn(Optional.of("_KAFKA_TIMESTAMP"));

    long recordTimestamp = 123456789L;
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            keySchema,
            keyStruct,
            valueSchema,
            valueStruct,
            OFFSET,
            recordTimestamp,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals("UPSERT", actual.get("_CHANGE_TYPE"));
    assertEquals(
        "00000000075BCD15/00000001/000000000000002A", actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowWithCustomSeqFallbackToTimestamp() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getCdcChangeSequenceNumberField()).thenReturn(Optional.of("updated_at"));

    long recordTimestamp = 9876543210L;
    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            keySchema,
            keyStruct,
            null,
            null,
            OFFSET,
            recordTimestamp,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals("DELETE", actual.get("_CHANGE_TYPE"));
    assertEquals(
        "000000024CB016EA/00000001/000000000000002A", actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowWithTimestampString() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getCdcChangeSequenceNumberField()).thenReturn(Optional.of("timestamp_str"));

    Schema timeValueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("timestamp_str", Schema.STRING_SCHEMA)
            .build();

    Struct timeValueStruct =
        new Struct(timeValueSchema).put("id", 123L).put("timestamp_str", "2026-08-14T10:00:00Z");

    SinkRecord record =
        new SinkRecord(
            TOPIC, PARTITION, keySchema, keyStruct, timeValueSchema, timeValueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals("UPSERT", actual.get("_CHANGE_TYPE"));
    long expectedEpochMs = java.time.Instant.parse("2026-08-14T10:00:00Z").toEpochMilli();
    assertEquals(
        String.format("%016X/%08X/%016X", expectedEpochMs, PARTITION, OFFSET),
        actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowDeleteRewrite() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    Schema rewriteValueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("version_id", Schema.INT64_SCHEMA)
            .field(DELETED_PSEUDO_COLUMN, Schema.BOOLEAN_SCHEMA)
            .build();

    Struct rewriteValueStruct =
        new Struct(rewriteValueSchema)
            .put("id", 123L)
            .put("name", "Alice")
            .put("version_id", 3L)
            .put(DELETED_PSEUDO_COLUMN, true);

    SinkRecord record =
        new SinkRecord(
            TOPIC, PARTITION, keySchema, keyStruct, rewriteValueSchema, rewriteValueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals(CDC_CHANGE_TYPE_DELETE, actual.get(CDC_CHANGE_TYPE_FIELD));
    assertEquals("00000001/000000000000002A", actual.get(CDC_CHANGE_SEQUENCE_NUMBER_FIELD));
  }

  @Test
  public void testCdcRowDeleteRewriteString() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    Schema rewriteValueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("version_id", Schema.INT64_SCHEMA)
            .field(DELETED_PSEUDO_COLUMN, Schema.STRING_SCHEMA)
            .build();

    Struct rewriteValueStruct =
        new Struct(rewriteValueSchema)
            .put("id", 123L)
            .put("name", "Alice")
            .put("version_id", 3L)
            .put(DELETED_PSEUDO_COLUMN, "true");

    SinkRecord record =
        new SinkRecord(
            TOPIC, PARTITION, keySchema, keyStruct, rewriteValueSchema, rewriteValueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertEquals(CDC_CHANGE_TYPE_DELETE, actual.get(CDC_CHANGE_TYPE_FIELD));
    assertEquals("00000001/000000000000002A", actual.get(CDC_CHANGE_SEQUENCE_NUMBER_FIELD));
  }

  @Test
  public void testCdcRowStripsDeletedField() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    Schema rewriteValueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("version_id", Schema.INT64_SCHEMA)
            .field(DELETED_PSEUDO_COLUMN, Schema.BOOLEAN_SCHEMA)
            .build();

    Struct rewriteValueStruct =
        new Struct(rewriteValueSchema)
            .put("id", 123L)
            .put("name", "Alice")
            .put("version_id", 3L)
            .put(DELETED_PSEUDO_COLUMN, true);

    SinkRecord record =
        new SinkRecord(
            TOPIC, PARTITION, keySchema, keyStruct, rewriteValueSchema, rewriteValueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    assertTrue(!actual.containsKey(DELETED_PSEUDO_COLUMN));
  }

  private static Map<String, Object> defaultExpectedFields() {
    return new HashMap<>(
        Map.of(
            SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
            kafkaDataTopicValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
            kafkaDataPartitionValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
            kafkaDataOffsetValue));
  }

  private static TestingBigQuerySinkConfig createConfig(Map<String, String> overrides) {
    Map<String, String> properties = new HashMap<>();
    properties.put("project", "project");
    properties.put("defaultDataset", "defaultDataset");
    properties.put("taskId", "1");
    properties.putAll(overrides);
    return new TestingBigQuerySinkConfig(properties);
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testGetRegularRowData")
  void testGetRegularRow(
      String name, BigQuerySinkConfig config, String ulid, Map<String, String> expected) {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            Map.of("one", "1", "two", "2"),
            kafkaDataOffsetValue);
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);
    Map<String, Object> regularRow = underTest.getRegularRow(record, ulid);
    assertEquals("1", regularRow.get("one"));
    assertEquals("2", regularRow.get("two"));
    if (expected == null) {
      assertNull(regularRow.get("kafkaDataFieldName"));
    } else {
      Map<String, String> actual = (Map<String, String>) regularRow.get("kafkaDataFieldName");
      assertNotNull(actual.get(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
      actual.remove(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME);
      assertEquals(expected, actual);
    }
  }

  static List<Arguments> testGetRegularRowData() {
    String putAttempt = ulid.nextULID();
    return List.of(
        Arguments.of(
            "+data+attempt",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            putAttempt,
            Map.of(
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataPartitionValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataOffsetValue,
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataTopicValue,
                SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME,
                putAttempt)),
        Arguments.of(
            "+data-null_attempt",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            null,
            Map.of(
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataPartitionValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataOffsetValue,
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataTopicValue)),
        Arguments.of(
            "+data-attempt",
            createConfig(
                Map.of(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")),
            putAttempt,
            Map.of(
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataPartitionValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataOffsetValue,
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataTopicValue)),
        Arguments.of(
            "-data+attempt",
            createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true")),
            putAttempt,
            null));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("testBuildKafkaDataRecordData")
  void testBuildKafkaDataRecord(
      String testId,
      BigQuerySinkConfig config,
      String putAttemptId,
      Map<String, String> expectedResults) {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);
    Map<String, Object> actualKafkaDataFields =
        underTest.buildKafkaDataRecord(record, putAttemptId);

    // time field is calculated when created so verify it exists and remove it before comparing with
    // expectedKafakDataFields
    assertTrue(actualKafkaDataFields.containsKey(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
    assertInstanceOf(
        Double.class, actualKafkaDataFields.get(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
    actualKafkaDataFields.remove(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME);

    assertEquals(expectedResults, actualKafkaDataFields);
  }

  static List<Arguments> testBuildKafkaDataRecordData() {
    String putAttemptId = ulid.nextULID();
    Map<String, Object> expectedKafkaDataFields = defaultExpectedFields();
    expectedKafkaDataFields.put(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);

    return List.of(
        Arguments.of(
            "true-id",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            putAttemptId,
            expectedKafkaDataFields),
        Arguments.of(
            "true-null",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            null,
            defaultExpectedFields()),
        Arguments.of(
            "false-id",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "false",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            putAttemptId,
            defaultExpectedFields()),
        Arguments.of(
            "false-null",
            createConfig(
                Map.of(
                    BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                    "false",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                    "kafkaDataFieldName")),
            putAttemptId,
            defaultExpectedFields()));
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("mutatedDataTestData")
  void mutatedDataTest(String name, BigQuerySinkConfig config, Map<String, Object> expectedData) {
    SinkRecord record =
        new SinkRecord(
            kafkaDataMutatedTopicValue,
            kafkaDataMutatedPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataMutatedOffsetValue,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME,
            Collections.emptyList(),
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            kafkaDataOffsetValue);

    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);
    Map<String, Object> actualKafkaDataFields =
        underTest.buildKafkaDataRecord(record, ulid.nextULID());
    actualKafkaDataFields.remove(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME);
    assertEquals(expectedData, actualKafkaDataFields);
  }

  static List<Arguments> mutatedDataTestData() {
    List<Arguments> result = new ArrayList<>();
    TestingBigQuerySinkConfig config =
        createConfig(
            Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "true"));
    config.setPost3_6Flag(false);
    result.add(
        Arguments.of(
            "preserve-<3.6",
            config,
            Map.of(
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataTopicValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataOffsetValue,
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataPartitionValue)));

    config =
        createConfig(
            Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "true"));
    config.setPost3_6Flag(true);
    result.add(
        Arguments.of(
            "preserve-3.6+",
            config,
            Map.of(
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataTopicValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataOffsetValue,
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataPartitionValue)));

    config =
        createConfig(
            Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "false"));
    config.setPost3_6Flag(false);
    result.add(
        Arguments.of(
            "not preserve-<3.6",
            config,
            Map.of(
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataMutatedTopicValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataMutatedOffsetValue,
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataMutatedPartitionValue)));

    config =
        createConfig(
            Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "false"));
    config.setPost3_6Flag(true);
    result.add(
        Arguments.of(
            "not preserve-3.6+",
            config,
            Map.of(
                SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME,
                kafkaDataMutatedTopicValue,
                SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
                kafkaDataMutatedOffsetValue,
                SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
                kafkaDataMutatedPartitionValue)));

    return result;
  }

  // ---- trackPutAttempts tests ----

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_includesPutAttemptId() {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config =
        createConfig(
            Map.of(
                BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                "true",
                BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, "attempt-abc");

    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertEquals("attempt-abc", result.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertInstanceOf(Double.class, result.get(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_flagDisabled_excludesPutAttemptId() {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config =
        createConfig(
            Map.of(
                BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                "false",
                BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, "attempt-abc");

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_nullAttemptId_excludesPutAttemptId() {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config =
        createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, null);

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_twoAttemptsProduceDifferentIds() {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config =
        createConfig(
            Map.of(
                BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                "true",
                BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);

    Map<String, Object> row1 = underTest.buildKafkaDataRecord(record, "attempt-1");
    Map<String, Object> row2 = underTest.buildKafkaDataRecord(record, "attempt-2");

    assertNotEquals(
        row1.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME),
        row2.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_noArgOverload_backwardCompatible() {
    SinkRecord record =
        new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config =
        createConfig(
            Map.of(
                BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG,
                "false",
                BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG,
                "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, ulid.nextULID());

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
  }
}
