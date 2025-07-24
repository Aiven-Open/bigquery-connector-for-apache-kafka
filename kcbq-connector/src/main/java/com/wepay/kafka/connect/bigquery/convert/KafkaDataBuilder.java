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
import java.util.HashMap;
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
    Field.Builder insertTimeBuilder = com.google.cloud.bigquery.Field.newBuilder(
            KAFKA_DATA_INSERT_TIME_FIELD_NAME, LegacySQLTypeName.TIMESTAMP)
        .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE);
    Field insertTimeField = insertTimeBuilder.build();

    return Field.newBuilder(kafkaDataFieldName, LegacySQLTypeName.RECORD,
            topicField, partitionField, offsetField, insertTimeField)
        .setMode(com.google.cloud.bigquery.Field.Mode.NULLABLE).build();
  }

  private static String tryGetOriginalTopic(SinkRecord kafkaConnectRecord) {
    try {
      return kafkaConnectRecord.originalTopic();
    } catch (NoSuchMethodError e) {
      logger.warn("This connector doesn't support SMTs that mutate the original topic name. This functionality is "
              + "available since version 3.6 of Kafka Connect");
      return kafkaConnectRecord.topic();
    }
  }

  private static Integer tryGetOriginalKafkaPartition(SinkRecord kafkaConnectRecord) {
    try {
      return kafkaConnectRecord.originalKafkaPartition();
    } catch (NoSuchMethodError e) {
      logger.warn("This connector doesn't support SMTs that mutate the original partition. This functionality is "
              + "available since version 3.6 of Kafka Connect");
      return kafkaConnectRecord.kafkaPartition();
    }
  }

  private static long tryGetOriginalKafkaOffset(SinkRecord kafkaConnectRecord) {
    try {
      return kafkaConnectRecord.originalKafkaOffset();
    } catch (NoSuchMethodError e) {
      logger.warn("This connector doesn't support SMTs that mutate the original offset. This functionality is "
              + "available since version 3.6 of Kafka Connect");
      return kafkaConnectRecord.kafkaOffset();
    }
  }

  /**
   * Construct a map of Kafka Data record
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @return HashMap which contains the values of kafka topic, partition, offset, and insertTime.
   */
  public static Map<String, Object> buildKafkaDataRecord(SinkRecord kafkaConnectRecord) {
    HashMap<String, Object> kafkaData = new HashMap<>();
    kafkaData.put(KAFKA_DATA_TOPIC_FIELD_NAME, tryGetOriginalTopic(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_PARTITION_FIELD_NAME, tryGetOriginalKafkaPartition(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_OFFSET_FIELD_NAME, tryGetOriginalKafkaOffset(kafkaConnectRecord));
    kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
    return kafkaData;
  }

  /**
   * Construct a map of Kafka Data record for sending to Storage Write API
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @return HashMap which contains the values of kafka topic, partition, offset, and insertTime in microseconds.
   */
  public static Map<String, Object> buildKafkaDataRecordStorageApi(SinkRecord kafkaConnectRecord) {
    Map<String, Object> kafkaData = buildKafkaDataRecord(kafkaConnectRecord);
    kafkaData.put(KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() * 1000);
    return kafkaData;
  }

}
