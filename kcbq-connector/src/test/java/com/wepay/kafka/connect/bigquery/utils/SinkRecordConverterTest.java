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


import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import de.huxhorn.sulky.ulid.ULID;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SinkRecordConverterTest {

  private static final String kafkaDataTopicValue = "testTopic";
  private static final int kafkaDataPartitionValue = 101;
  private static final long kafkaDataOffsetValue = 1337;
  private static final String kafkaDataMutatedTopicValue = "mutatedTopic";
  private static final int kafkaDataMutatedPartitionValue = 201;
  private static final long kafkaDataMutatedOffsetValue = 456;
  private static final ULID ulid = new ULID();


  private static Map<String, Object> defaultExpectedFields() {
    return new HashMap<>(Map.of(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue));
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
  void testGetRegularRow(String name, BigQuerySinkConfig config, String ulid, Map<String, String> expected) {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, Map.of("one", "1", "two", "2"), kafkaDataOffsetValue);
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
            Arguments.of("+data+attempt", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), putAttempt,
                    Map.of(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue,
                            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue,
                            SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue,
                            SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttempt)),
            Arguments.of("+data-null_attempt", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
                            BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), null,
                    Map.of(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue,
                            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue,
                            SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue)),
            Arguments.of("+data-attempt", createConfig(Map.of(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), putAttempt,
                    Map.of(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue,
                            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue,
                            SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue)),

            Arguments.of("-data+attempt", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true")), putAttempt, null)
            );
  }

  @ParameterizedTest(name="{index} {0}")
  @MethodSource("testBuildKafkaDataRecordData")
  void testBuildKafkaDataRecord(String testId, BigQuerySinkConfig config, String putAttemptId, Map<String, String> expectedResults) {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    SinkRecordConverter underTest = new SinkRecordConverter(config, null, null);
    Map<String, Object> actualKafkaDataFields = underTest.buildKafkaDataRecord(record, putAttemptId);

    // time field is calculated when created so verify it exists and remove it before comparing with expectedKafakDataFields
    assertTrue(actualKafkaDataFields.containsKey(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
    assertInstanceOf(Double.class, actualKafkaDataFields.get(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
    actualKafkaDataFields.remove(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME);

    assertEquals(expectedResults, actualKafkaDataFields);
  }

  static List<Arguments> testBuildKafkaDataRecordData() {
    String putAttemptId = ulid.nextULID();
    Map<String, Object> expectedKafkaDataFields = defaultExpectedFields();
    expectedKafkaDataFields.put(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);

    return List.of(
            Arguments.of("true-id", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), putAttemptId, expectedKafkaDataFields),
            Arguments.of("true-null", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), null, defaultExpectedFields()),
            Arguments.of("false-id", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "false",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), putAttemptId, defaultExpectedFields()),
            Arguments.of("false-null", createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "false",
                    BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName")), putAttemptId, defaultExpectedFields())
    );
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("mutatedDataTestData")
  void mutatedDataTest(String name, BigQuerySinkConfig config,  Map<String,Object> expectedData) {
    SinkRecord record = new  SinkRecord(kafkaDataMutatedTopicValue, kafkaDataMutatedPartitionValue, null, null, null, null,
    kafkaDataMutatedOffsetValue, System.currentTimeMillis(), TimestampType.CREATE_TIME, Collections.emptyList(), kafkaDataTopicValue,
    kafkaDataPartitionValue, kafkaDataOffsetValue);


    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);
    Map<String, Object> actualKafkaDataFields = underTest.buildKafkaDataRecord(record, ulid.nextULID());
    actualKafkaDataFields.remove(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME);
    assertEquals(expectedData, actualKafkaDataFields);
  }

  static List<Arguments> mutatedDataTestData() {
    List<Arguments> result = new ArrayList<>();
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "true"));
    config.setPost3_6Flag(false);
    result.add(Arguments.of("preserve-<3.6", config, Map.of(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue)));

    config = createConfig(Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "true"));
    config.setPost3_6Flag(true);
    result.add(Arguments.of("preserve-3.6+", config, Map.of(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataTopicValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataOffsetValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataPartitionValue)));

    config = createConfig(Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "false"));
    config.setPost3_6Flag(false);
    result.add(Arguments.of("not preserve-<3.6", config, Map.of(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataMutatedTopicValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataMutatedOffsetValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataMutatedPartitionValue)));

    config = createConfig(Map.of(BigQuerySinkConfig.PRESERVE_KAFKA_TOPIC_PARTITION_OFFSET__CONFIG, "false"));
    config.setPost3_6Flag(true);
    result.add(Arguments.of("not preserve-3.6+", config, Map.of(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, kafkaDataMutatedTopicValue,
            SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, kafkaDataMutatedOffsetValue,
            SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, kafkaDataMutatedPartitionValue)));

    return result;
  }

  // ---- trackPutAttempts tests ----

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_includesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
            BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, "attempt-abc");

    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertEquals("attempt-abc", result.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertInstanceOf(Double.class, result.get(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_flagDisabled_excludesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "false",
            BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, "attempt-abc");

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_flagEnabled_nullAttemptId_excludesPutAttemptId() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, null);

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
  }

  @Test
  public void testBuildKafkaDataRecord_twoAttemptsProduceDifferentIds() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true",
            BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);

    Map<String, Object> row1 = underTest.buildKafkaDataRecord(record, "attempt-1");
    Map<String, Object> row2 = underTest.buildKafkaDataRecord(record, "attempt-2");

    assertNotEquals(
        row1.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME),
        row2.get(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME)
    );
  }


  @Test
  public void testBuildKafkaDataRecord_noArgOverload_backwardCompatible() {
    SinkRecord record = new SinkRecord(kafkaDataTopicValue, kafkaDataPartitionValue, null, null, null, null, kafkaDataOffsetValue);
    TestingBigQuerySinkConfig config = createConfig(Map.of(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "false",
            BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "kafkaDataFieldName"));
    SinkRecordConverter underTest = new SinkRecordConverter(config, null,  null);

    Map<String, Object> result = underTest.buildKafkaDataRecord(record, ulid.nextULID());

    assertFalse(result.containsKey(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME));
    assertTrue(result.containsKey(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME));
  }

}
