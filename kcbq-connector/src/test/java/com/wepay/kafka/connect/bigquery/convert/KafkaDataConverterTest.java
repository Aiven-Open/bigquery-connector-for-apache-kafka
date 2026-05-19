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

package com.wepay.kafka.connect.bigquery.convert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class KafkaDataConverterTest {

  public static final String kafkaDataFieldName = "kafkaData";
  private static final String kafkaDataTopicName = "topic";
  private static final String kafkaDataPartitionName = "partition";
  private static final String kafkaDataOffsetName = "offset";
  private static final String kafkaDataInsertTimeName = "insertTime";
  private static final String kafkaDataTopicValue = "testTopic";
  private static final int kafkaDataPartitionValue = 101;
  private static final long kafkaDataOffsetValue = 1337;
  private static final String kafkaDataMutatedTopicValue = "mutatedTopic";
  private static final int kafkaDataMutatedPartitionValue = 201;
  // In 3.6.1, there is no direct way to modify offset via newRecord(), even if SinkRecord itself supports it
  private static final long kafkaDataMutatedOffsetValue = 456;
  Map<String, Object> expectedKafkaDataFields = new HashMap<>();

  @BeforeEach
  public void setup() {
    expectedKafkaDataFields.put(kafkaDataTopicName, kafkaDataTopicValue);
    expectedKafkaDataFields.put(kafkaDataPartitionName, kafkaDataPartitionValue);
    expectedKafkaDataFields.put(kafkaDataOffsetName, kafkaDataOffsetValue);
    // Reset static flag so tests do not interfere with each other
    KafkaDataBuilder.setTrackPutAttempts(false);
  }

  @Test
  public void testBuildKafkaDataRecord() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecord(record);

    assertTrue(actualKafkaDataFields.containsKey(kafkaDataInsertTimeName));
    assertInstanceOf(Double.class, actualKafkaDataFields.get(kafkaDataInsertTimeName));

    actualKafkaDataFields.remove(kafkaDataInsertTimeName);

    assertEquals(expectedKafkaDataFields, actualKafkaDataFields);
  }

  @Test
  public void testBuildKafkaDataRecordOnMutatedMetadata() {
    SinkRecord record = new SinkRecord(
            kafkaDataTopicValue,
            kafkaDataPartitionValue,
            null,
            null,
            null,
            null,
            kafkaDataOffsetValue
    );
    SinkRecord mutatedRecord = record.newRecord(
            kafkaDataMutatedTopicValue,
            kafkaDataMutatedPartitionValue,
            null,
            null,
            null,
            null,
            null
    );

    KafkaDataBuilder.setUseOriginalValues(true);
    KafkaDataBuilder.setPost3_6Flag(true);
    Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecord(mutatedRecord);
    actualKafkaDataFields.remove(kafkaDataInsertTimeName);

    assertEquals(expectedKafkaDataFields, actualKafkaDataFields);
  }

  @ParameterizedTest
  @MethodSource("buildKafkaDataRecordTestData")
  void testBuildKafkaDataRecord(String ver, boolean useOriginalValues, boolean post3_6Flag, SinkRecord sinkRecord, Map<String, Object> expectedKafkaDataFields) {
    KafkaDataBuilder.setUseOriginalValues(useOriginalValues);
    KafkaDataBuilder.setPost3_6Flag(post3_6Flag);
    Map<String, Object> actualKafkaDataRecord = KafkaDataBuilder.buildKafkaDataRecord(sinkRecord);
    // remove any unwanted values.
    actualKafkaDataRecord.keySet().retainAll(expectedKafkaDataFields.keySet());
    assertEquals(expectedKafkaDataFields, actualKafkaDataRecord);
  }

  static List<Arguments> buildKafkaDataRecordTestData() {
    List<Arguments> arguments = new ArrayList<>();
    SinkRecord sinkRecord;
    Map<String, Object> expected;

    // pre 3.6 record format
    sinkRecord = new SinkRecord("topic", 1, null, null, null, null, 2L);
    expected = new HashMap<>();
    expected.put(kafkaDataTopicName, "topic");
    expected.put(kafkaDataPartitionName, 1);
    expected.put(kafkaDataOffsetName, 2L);
    arguments.add(Arguments.of("pre 3.6",true, false, sinkRecord, expected));
    arguments.add(Arguments.of("pre 3.6",true, true, sinkRecord, expected));
    arguments.add(Arguments.of("pre 3.6",false, false, sinkRecord, expected));
    arguments.add(Arguments.of("pre 3.6",false, true, sinkRecord, expected));

    // post 3.6 record format
    sinkRecord = new SinkRecord( "topic", 1, null, null, null, null, 2L,
            System.currentTimeMillis(), TimestampType.CREATE_TIME, null, "origTopic", 11, 22L);
    arguments.add(Arguments.of("post 3.6", true, false, sinkRecord, expected));
    arguments.add(Arguments.of("post 3.6", false, false, sinkRecord, expected));
    arguments.add(Arguments.of("post 3.6", false, true, sinkRecord, expected));
    expected = new HashMap<>();
    expected.put(kafkaDataTopicName, "origTopic");
    expected.put(kafkaDataPartitionName, 11);
    expected.put(kafkaDataOffsetName, 22L);
    arguments.add(Arguments.of("post 3.6", true, true, sinkRecord, expected));
    return arguments;
  }

  @Test
  public void testBuildKafkaDataRecordStorageWriteApi() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    Map<String, Object> actualKafkaDataFields = KafkaDataBuilder.buildKafkaDataRecordStorageApi(record);

    assertTrue(actualKafkaDataFields.containsKey(kafkaDataInsertTimeName));
    assertInstanceOf(Long.class, actualKafkaDataFields.get(kafkaDataInsertTimeName));

    actualKafkaDataFields.remove(kafkaDataInsertTimeName);

    assertEquals(expectedKafkaDataFields, actualKafkaDataFields);
  }

  @Test
  public void testBuildKafkaDataField() {
    Field topicField = Field.of("topic", LegacySQLTypeName.STRING);
    Field partitionField = Field.of("partition", LegacySQLTypeName.INTEGER);
    Field offsetField = Field.of("offset", LegacySQLTypeName.INTEGER);
    Field insertTimeField = Field.newBuilder("insertTime", LegacySQLTypeName.TIMESTAMP)
        .setMode(Field.Mode.NULLABLE)
        .build();

    Field expectedBigQuerySchema = Field.newBuilder(kafkaDataFieldName,
            LegacySQLTypeName.RECORD,
            topicField,
            partitionField,
            offsetField,
            insertTimeField)
        .setMode(Field.Mode.NULLABLE)
        .build();
    Field actualBigQuerySchema = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName);
    assertEquals(expectedBigQuerySchema, actualBigQuerySchema);
  }

  // ---- trackPutAttempts tests ----

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_includesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    KafkaDataBuilder.setTrackPutAttempts(true);

    Map<String, Object> result = KafkaDataBuilder.buildKafkaDataRecord(record, "attempt-abc");

    assertTrue(result.containsKey(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertEquals("attempt-abc", result.get(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertInstanceOf(Double.class, result.get(kafkaDataInsertTimeName));
  }

  @Test
  public void testBuildKafkaDataRecord_flagDisabled_excludesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    // flag already false from setup()

    Map<String, Object> result = KafkaDataBuilder.buildKafkaDataRecord(record, "attempt-abc");

    assertFalse(result.containsKey(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_nullAttemptId_excludesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    KafkaDataBuilder.setTrackPutAttempts(true);

    Map<String, Object> result = KafkaDataBuilder.buildKafkaDataRecord(record, null);

    assertFalse(result.containsKey(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_twoAttemptsProduceDifferentIds() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    KafkaDataBuilder.setTrackPutAttempts(true);

    Map<String, Object> row1 = KafkaDataBuilder.buildKafkaDataRecord(record, "attempt-1");
    Map<String, Object> row2 = KafkaDataBuilder.buildKafkaDataRecord(record, "attempt-2");

    assertNotEquals(
        row1.get(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME),
        row2.get(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME)
    );
  }

  @Test
  public void testBuildKafkaDataField_flagEnabled_includesPutAttemptIdSubfield() {
    KafkaDataBuilder.setTrackPutAttempts(true);

    Field recordField = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName);

    boolean found = recordField.getSubFields().stream()
        .anyMatch(f -> KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME.equals(f.getName())
            && LegacySQLTypeName.STRING.equals(f.getType())
            && Field.Mode.NULLABLE.equals(f.getMode()));
    assertTrue(found, "putAttemptId STRING NULLABLE subfield should be present when flag is enabled");
  }

  @Test
  public void testBuildKafkaDataField_flagDisabled_excludesPutAttemptIdSubfield() {
    // flag already false from setup()

    Field recordField = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName);

    boolean found = recordField.getSubFields().stream()
        .anyMatch(f -> KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME.equals(f.getName()));
    assertFalse(found, "putAttemptId subfield should not be present when flag is disabled");
  }

  @Test
  public void testBuildKafkaDataRecordStorageApi_flagEnabled_includesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    KafkaDataBuilder.setTrackPutAttempts(true);

    Map<String, Object> result = KafkaDataBuilder.buildKafkaDataRecordStorageApi(record, "attempt-xyz");

    assertTrue(result.containsKey(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertEquals("attempt-xyz", result.get(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertInstanceOf(Long.class, result.get(kafkaDataInsertTimeName));
    assertTrue((Long) result.get(kafkaDataInsertTimeName) > 0);
  }

  @Test
  public void testBuildKafkaDataRecord_noArgOverload_backwardCompatible() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    // flag is false from setup()

    Map<String, Object> result = KafkaDataBuilder.buildKafkaDataRecord(record);

    assertFalse(result.containsKey(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertTrue(result.containsKey(kafkaDataTopicName));
    assertTrue(result.containsKey(kafkaDataPartitionName));
    assertTrue(result.containsKey(kafkaDataOffsetName));
    assertTrue(result.containsKey(kafkaDataInsertTimeName));
  }
}
