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


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to construct schema and record for Kafka Data Field.
 */
public class KafkaDataBuilder {

  private static final Logger logger = LoggerFactory.getLogger(KafkaDataBuilder.class);

  public static final String KAFKA_DATA_TOPIC_FIELD_NAME = "topic";
  public static final String KAFKA_DATA_PARTITION_FIELD_NAME = "partition";
  public static final String KAFKA_DATA_OFFSET_FIELD_NAME = "offset";
  public static final String KAFKA_DATA_INSERT_TIME_FIELD_NAME = "insertTime";
  public static final String KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME = "putAttemptId";

  /**
   * This is a marker variable for methods necessary to keep original sink record metadata.
   * These methods in SinkRecord class are available only since Kafka Connect API version 3.6.
   */
  private static boolean KAFKA_CONNECT_API_POST_3_6;

  /**
   * This variable determines if the original variable or the mutated variable should be used
   * when running in a post 3.6 Kafka.
   */
  private static boolean USE_ORIGINAL_VALUES = false;

  /**
   * When true, a per-put() attempt ULID is embedded as {@code putAttemptId} inside the kafka
   * metadata struct, and the BQ schema for that struct includes the field. Controlled by
   * {@code TRACK_PUT_ATTEMPTS} connector config.
   */
  private static boolean TRACK_PUT_ATTEMPTS = false;

  static {
    boolean kafkaConnectApiPost36;
    try {
      MethodHandles.lookup().findVirtual(
          SinkRecord.class,
          "originalTopic",
          MethodType.methodType(String.class)
      );
      MethodHandles.lookup().findVirtual(
          SinkRecord.class,
          "originalKafkaPartition",
          MethodType.methodType(Integer.class)
      );
      MethodHandles.lookup().findVirtual(
          SinkRecord.class,
          "originalKafkaOffset",
          MethodType.methodType(long.class)
      );
      kafkaConnectApiPost36 = true;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      logger.warn("This connector cannot retain original topic/partition/offset fields in SinkRecord. "
                + "If these fields are mutated in upstream SMTs, they will be lost. "
                + "Upgrade to Kafka Connect 3.6 to provision reliable metadata into resulting table.", e);
      kafkaConnectApiPost36 = false;
    }
    KAFKA_CONNECT_API_POST_3_6 = kafkaConnectApiPost36;
  }

  /**
   * Sets the use original values flag.
   *
   * @param useOriginalValues the state of the flag.
   */
  public static void setUseOriginalValues(boolean useOriginalValues) {
    USE_ORIGINAL_VALUES = useOriginalValues;
  }

  /**
   * Sets the put-attempt tracking flag. When true, {@link #buildKafkaDataField} includes a
   * {@code putAttemptId} subfield in the BQ schema, and {@link #buildKafkaDataRecord(SinkRecord,
   * String)} embeds the supplied attempt ID in the row map. Called from
   * {@link com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig} constructor.
   *
   * @param track whether to track put() attempt IDs.
   */
  public static void setTrackPutAttempts(boolean track) {
    TRACK_PUT_ATTEMPTS = track;
  }

  /**
   * Sets the Kafka Post 3.6 flag.  Used in testing.
   *
   * @param post36Flag the state of the flag.
   */
  @VisibleForTesting
  static void setPost3_6Flag(boolean post36Flag) {
    KAFKA_CONNECT_API_POST_3_6 = post36Flag;
  }

  /**
   * Construct schema for Kafka Data Field
   *
   * @param kafkaDataFieldName The configured name of Kafka Data Field
   * @return Field of Kafka Data, with definitions of kafka topic, partition, offset, and insertTime.
   */
  public static Field buildKafkaDataField(String kafkaDataFieldName) {
    Field topicField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_TOPIC_FIELD_NAME, LegacySQLTypeName.STRING);
    Field partitionField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_PARTITION_FIELD_NAME, LegacySQLTypeName.INTEGER);
    Field offsetField = com.google.cloud.bigquery.Field.of(KAFKA_DATA_OFFSET_FIELD_NAME, LegacySQLTypeName.INTEGER);
    Field insertTimeField = com.google.cloud.bigquery.Field.newBuilder(
            KAFKA_DATA_INSERT_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
        .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build();

    List<Field> subFields = new ArrayList<>(
        Arrays.asList(topicField, partitionField, offsetField, insertTimeField));

    if (TRACK_PUT_ATTEMPTS) {
      subFields.add(com.google.cloud.bigquery.Field.newBuilder(
              KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, LegacySQLTypeName.STRING)
          .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build());
    }

    return Field.newBuilder(kafkaDataFieldName, LegacySQLTypeName.RECORD,
            subFields.toArray(new Field[0]))
        .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build();
  }

  private static String maybeGetOriginalTopic(SinkRecord kafkaConnectRecord) {
    if (KAFKA_CONNECT_API_POST_3_6 && USE_ORIGINAL_VALUES) {
      return kafkaConnectRecord.originalTopic();
    } else {
      return kafkaConnectRecord.topic();
    }
  }

  private static Integer maybeGetOriginalKafkaPartition(SinkRecord kafkaConnectRecord) {
    if (KAFKA_CONNECT_API_POST_3_6 && USE_ORIGINAL_VALUES) {
      return kafkaConnectRecord.originalKafkaPartition();
    } else {
      return kafkaConnectRecord.kafkaPartition();
    }
  }

  private static long maybeGetOriginalKafkaOffset(SinkRecord kafkaConnectRecord) {
    if (KAFKA_CONNECT_API_POST_3_6 && USE_ORIGINAL_VALUES) {
      return kafkaConnectRecord.originalKafkaOffset();
    } else {
      return kafkaConnectRecord.kafkaOffset();
    }
  }

  /**
   * Construct a map of Kafka Data record. Backward-compatible overload; does not include
   * {@code putAttemptId} even when {@code TRACK_PUT_ATTEMPTS} is enabled.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @return HashMap which contains the values of kafka topic, partition, offset, and insertTime.
   */
  public static Map<String, Object> buildKafkaDataRecord(SinkRecord kafkaConnectRecord) {
    return buildKafkaDataRecord(kafkaConnectRecord, null);
  }

  /**
   * Construct a map of Kafka Data record, optionally including a put-attempt identifier.
   *
   * <p>When {@code TRACK_PUT_ATTEMPTS} is enabled and {@code putAttemptId} is non-null, the map
   * includes a {@code putAttemptId} entry so that rows constructed during different
   * {@code put()} invocations can be distinguished downstream.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @param putAttemptId ULID string generated at the start of the enclosing {@code put()} call,
   *                     or {@code null} to omit the field.
   * @return HashMap which contains the values of kafka topic, partition, offset, insertTime,
   *         and optionally putAttemptId.
   */
  public static Map<String, Object> buildKafkaDataRecord(SinkRecord kafkaConnectRecord,
                                                         String putAttemptId) {
    HashMap<String, Object> kafkaData = new HashMap<>();
    kafkaData.put(KAFKA_DATA_TOPIC_FIELD_NAME, maybeGetOriginalTopic(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_PARTITION_FIELD_NAME, maybeGetOriginalKafkaPartition(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_OFFSET_FIELD_NAME, maybeGetOriginalKafkaOffset(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
    if (TRACK_PUT_ATTEMPTS && putAttemptId != null) {
      kafkaData.put(KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);
    }
    return kafkaData;
  }

  /**
   * Construct a map of Kafka Data record for sending to Storage Write API.
   * Backward-compatible overload; does not include {@code putAttemptId}.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @return HashMap which contains the values of kafka topic, partition, offset, and insertTime in microseconds.
   */
  public static Map<String, Object> buildKafkaDataRecordStorageApi(SinkRecord kafkaConnectRecord) {
    return buildKafkaDataRecordStorageApi(kafkaConnectRecord, null);
  }

  /**
   * Construct a map of Kafka Data record for sending to Storage Write API, optionally including
   * a put-attempt identifier.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @param putAttemptId ULID string generated at the start of the enclosing {@code put()} call,
   *                     or {@code null} to omit the field.
   * @return HashMap which contains the values of kafka topic, partition, offset, insertTime in
   *         microseconds, and optionally putAttemptId.
   */
  public static Map<String, Object> buildKafkaDataRecordStorageApi(SinkRecord kafkaConnectRecord,
                                                                   String putAttemptId) {
    HashMap<String, Object> kafkaData = new HashMap<>();
    kafkaData.put(KAFKA_DATA_TOPIC_FIELD_NAME, maybeGetOriginalTopic(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_PARTITION_FIELD_NAME, maybeGetOriginalKafkaPartition(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_OFFSET_FIELD_NAME, maybeGetOriginalKafkaOffset(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() * 1000);
    if (TRACK_PUT_ATTEMPTS && putAttemptId != null) {
      kafkaData.put(KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);
    }
    return kafkaData;
  }

}
