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

package com.wepay.kafka.connect.bigquery.integration;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import io.confluent.connect.avro.AvroConverter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryErrantRecordHandlerIT extends BaseConnectorIT {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryErrantRecordHandlerIT.class);
  private static final String CONNECTOR_NAME = "kcbq-sink-connector";
  private static final int NUM_RECORDS_PRODUCED = 20;
  private static SchemaRegistryTestUtils schemaRegistry;
  private static String schemaRegistryUrl;
  private BigQuery bigQuery;
  private Converter converter;

  private org.apache.kafka.connect.data.Schema valueSchema;

  @BeforeEach
  void setup() throws Exception {
    startConnect();
    bigQuery = newBigQuery();

    schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
    schemaRegistry.start();
    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

    valueSchema =
        SchemaBuilder.struct()
            .optional()
            .field("f1", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .field("f2", org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA)
            .field("f3", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
            .build();
  }

  @AfterEach
  void close() throws Exception {
    delete(bigQuery, tableName());
    bigQuery = null;
    stopConnect();
    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
  }

  private String dlqTopic() {
    return topicName() + "_dlq_topic";
  }

  @Test
  public void testRecordsSentToDlqOnInvalidArgumentAvroStorageApi() throws Exception {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();

    createTopicAndTable();
    Map<String, String> props = connectorAvroProps(topic, dlqTopic);

    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    converter = new AvroConverter();
    converter.configure(
        Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

    List<SchemaAndValue> records = getRecords();
    // generating invalid records which leads to INVALID_ARGUMENT error on data ingestion
    schemaRegistry.produceRecords(converter, records, topic);

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(2), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnInvalidArgumentStorageApi() throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();

    createTopicAndTable();
    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send Invalid records to BigQuery
    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> key(keyConverter, topic, k), v -> value(valueConverter, topic, v));

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(2), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnRecordConversionErrorStorageApi() throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, 1);

    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Send Invalid records to Kafka
    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> "key-" + k, v -> "\"f1\":1");

    verify(dlqTopic, Duration.ofMinutes(2), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnInvalidArgumentAvroBatchStorageApi() throws Exception {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();
    int recordCount = 2;

    createTopicAndTable();
    Map<String, String> props = connectorAvroProps(topic, dlqTopic);
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    converter = new AvroConverter();
    converter.configure(
        Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

    List<SchemaAndValue> records = getRecords(recordCount);
    schemaRegistry.produceRecords(converter, records, topic);

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(3), recordCount);
  }

  @Test
  public void testRecordsSentToDlqOnInvalidArgumentBatchStorageApi() throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();
    createTopicAndTable();
    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> key(keyConverter, topic, k), v -> value(valueConverter, topic, v));

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(3), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnRecordConversionErrorBatchStorageApi()
      throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();

    createTopicAndTable();
    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG, "true");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);


    // Send Invalid records to Kafka
    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> "key-" + k, v -> "\"f1\":1");

    verify(dlqTopic, Duration.ofSeconds(180), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnInvalidReasonAvro() throws Exception {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();

    createTopicAndTable();
    Map<String, String> props = connectorAvroProps(topic, dlqTopic);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    converter = new AvroConverter();
    converter.configure(
        Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

    List<SchemaAndValue> records = getRecords();
    schemaRegistry.produceRecords(converter, records, topic);

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(2), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnInvalidReason() throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();

    createTopicAndTable();
    Map<String, String> props = connectorProps(topic, dlqTopic);

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Instantiate the converters we'll use to send records to the connector
    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Send Invalid records to BigQuery
    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> key(keyConverter, topic, k), v -> value(valueConverter, topic, v));

    // Check records show up in dlq topic
    verify(dlqTopic, Duration.ofMinutes(2), NUM_RECORDS_PRODUCED);
  }

  @Test
  public void testRecordsSentToDlqOnRecordConversionError() throws InterruptedException {
    final String topic = topicName();
    final String dlqTopic = dlqTopic();
    // Make sure each task gets to read from at least one partition
    connect.kafka().createTopic(topic, 1);

    Map<String, String> props = connectorProps(topic, dlqTopic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put("key.converter.schemas.enable", "false");
    props.put("value.converter.schemas.enable", "false");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);

    // Send Invalid records to Kafka
    sendMessages(topic, NUM_RECORDS_PRODUCED, k -> "key-" + k, v -> "\"f1\":1");

    verify(dlqTopic, Duration.ofSeconds(2), NUM_RECORDS_PRODUCED);
  }

  private void sendMessages(String topic, int count, IntFunction<String> keyFunc, IntFunction<String> valueFunc) {
    logger.debug(
            "Sending messages with keys ['{}', '{}']  and value ['{}', '{}'] to topic '{}'",
            keyFunc.apply(0),
            keyFunc.apply(count-1),
            valueFunc.apply(0),
            valueFunc.apply(count-1),
            topic);
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      connect.kafka().produce(topic, keyFunc.apply(i), valueFunc.apply(i));
    }
  }

  private Map<String, String> connectorProps(String topicName, String dlqTopicName) {
    Map<String, String> result = baseConnectorProps(1);
    result.put(SinkConnectorConfig.TOPICS_CONFIG, topicName);

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // DLQ Error Handler Configs
    result.put(SinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
    result.put(SinkConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
    result.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, dlqTopicName);
    result.put(SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    result.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");

    return result;
  }

  private Map<String, String> connectorAvroProps(String topicName, String dlqTopicName) {
    Map<String, String> result = baseConnectorProps(1);
    result.put(SinkConnectorConfig.TOPICS_CONFIG, topicName);

    // use the Avro converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
        ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);
    result.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    result.put(
        ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistryUrl);

    // DLQ Error Handler Configs
    result.put(SinkConnectorConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
    result.put(SinkConnectorConfig.ERRORS_TOLERANCE_CONFIG, "all");
    result.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, dlqTopicName);
    result.put(SinkConnectorConfig.DLQ_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
    result.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");

    return result;
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String key(Converter converter, String topic, long iteration) {
    final org.apache.kafka.connect.data.Schema schema =
        SchemaBuilder.struct()
            .field("k1", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
            .build();

    final Struct struct = new Struct(schema).put("k1", iteration);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String value(Converter converter, String topic, int iteration) {
    return new String(converter.fromConnectData(topic, valueSchema, data(iteration)));
  }

  private Struct data(int iteration) {
    return new Struct(valueSchema)
        .put("f1", iteration % 2 == 0 ? "a string" : "another string")
        .put("f2", iteration % 3 == 0)
        .put("f3", "invalid value according to table schema");
  }

  private List<SchemaAndValue> getRecords() {
    return getRecords(NUM_RECORDS_PRODUCED);
  }

  private List<SchemaAndValue> getRecords(int recordCount) {
    List<SchemaAndValue> recordList = new ArrayList<>();
    for (int i = 0; i < recordCount; i++) {
      SchemaAndValue schemaAndValue = new SchemaAndValue(valueSchema, data(i));
      recordList.add(schemaAndValue);
    }
    return recordList;
  }

  private void createTopicAndTable() {
    connect.kafka().createTopic(topicName());

    final TableName tableName = tableName();
    // Create table schema
    Schema schema =
            Schema.of(
                    Field.of("f1", StandardSQLTypeName.STRING),
                    Field.of("f2", StandardSQLTypeName.BOOL),
                    Field.of("f3", StandardSQLTypeName.INT64));

    // Try to create BigQuery table
    try {
      BigQueryTestUtils.createPartitionedTable(bigQuery, tableName, schema);
    } catch (BigQueryException ex) {
      fail("Failed to create table: " + tableName, ex);
    }
  }

  private void verify(String dlqTopic, Duration duration, int recordCount) {
    ConsumerRecords<byte[], byte[]> records =
            connect.kafka().consume(recordCount, duration.toMillis(), dlqTopic);

    if (logger.isDebugEnabled() && records.count() != recordCount) {
      records.partitions().forEach(tp -> logger.debug("topic {} partition {}", tp.topic(), tp.partition()));
      records.records(dlqTopic).forEach( cr -> logger.debug("value: {}", new String(cr.value())));
    }
    assertThat(records.count()).isEqualTo(recordCount);
  }
}
