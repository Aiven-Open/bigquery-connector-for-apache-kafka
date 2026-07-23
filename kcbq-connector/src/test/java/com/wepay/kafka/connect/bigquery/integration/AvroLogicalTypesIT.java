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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import io.confluent.connect.avro.AvroConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies Avro logical time types (timestamp-micros, timestamp-nanos,
 * time-micros, local-timestamp-*) are written to BigQuery with the correct column types and values,
 * rather than falling through to plain INTEGER.
 *
 * <p>These types are passed through by the Confluent schema registry AvroData converter as named
 * INT64 Connect schemas, and are handled by {@link
 * com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters}.
 */
@Tag("integration")
public class AvroLogicalTypesIT extends BaseConnectorIT {

  private static final String CONNECTOR_NAME = "bigquery-avro-logical-types-connector";
  private static final int TASKS_MAX = 1;

  // 2017-03-01 22:20:38.808123 UTC
  private static final long TIMESTAMP_MICROS = 1_488_406_838_808_123L;
  private static final long TIMESTAMP_NANOS = TIMESTAMP_MICROS * 1_000L;
  // 2017-03-01 22:20:38.808 UTC in milliseconds
  private static final long TIMESTAMP_MILLIS = TIMESTAMP_MICROS / 1_000L;
  // 22:20:38.808123 as microseconds since midnight
  private static final long TIME_MICROS = (22 * 3_600L + 20 * 60L + 38) * 1_000_000L + 808_123L;

  private BigQuery bigQuery;
  private SchemaRegistryTestUtils schemaRegistry;
  private Converter keyConverter;
  private Converter valueConverter;

  private org.apache.kafka.connect.data.Schema keySchema;
  private org.apache.kafka.connect.data.Schema valueSchema;

  @BeforeEach
  public void setup() throws Exception {
    startConnect();
    bigQuery = newBigQuery();

    schemaRegistry = new SchemaRegistryTestUtils(connect.kafka().bootstrapServers());
    schemaRegistry.start();

    keySchema = SchemaBuilder.struct()
        .field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
        .build();

    valueSchema = SchemaBuilder.struct()
        .field("ts_micros", SchemaBuilder.int64().name("timestamp-micros").optional().build())
        .field("ts_nanos", SchemaBuilder.int64().name("timestamp-nanos").optional().build())
        .field("local_ts_millis", SchemaBuilder.int64().name("local-timestamp-millis").optional().build())
        .field("local_ts_micros", SchemaBuilder.int64().name("local-timestamp-micros").optional().build())
        .field("local_ts_nanos", SchemaBuilder.int64().name("local-timestamp-nanos").optional().build())
        .field("time_micros", SchemaBuilder.int64().name("time-micros").optional().build())
        .build();

    keyConverter = new AvroConverter();
    keyConverter.configure(
        Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.schemaRegistryUrl()),
        true
    );

    valueConverter = new AvroConverter();
    valueConverter.configure(
        Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.schemaRegistryUrl()),
        false
    );
  }

  @AfterEach
  public void cleanup() throws Exception {
    connect.deleteConnector(CONNECTOR_NAME);
    if (schemaRegistry != null) {
      schemaRegistry.stop();
    }
    stopConnect();
  }

  @Test
  public void testAvroLogicalTypesLandWithCorrectBigQueryTypes() throws Exception {
    final String topic = suffixedTableOrTopic("avro-logical-types");
    final String table = sanitizedTable(topic);

    connect.kafka().createTopic(topic, TASKS_MAX);
    TableClearer.clearTables(bigQuery, dataset(), table);

    connect.configureConnector(CONNECTOR_NAME, connectorProps(topic));
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    produceRecord(topic, 1L);

    waitForCommittedRecords(
        CONNECTOR_NAME, Collections.singleton(topic), 1, TASKS_MAX, COMMIT_MAX_DURATION_MS);

    // Verify BigQuery column types — the core assertion:
    // before this change these would all be INTEGER (plain INT64 fallback)
    Schema bqSchema = getBigQuerySchema(table);
    assertFieldType(bqSchema, "ts_micros", LegacySQLTypeName.TIMESTAMP);
    assertFieldType(bqSchema, "ts_nanos", LegacySQLTypeName.TIMESTAMP);
    assertFieldType(bqSchema, "local_ts_millis", LegacySQLTypeName.DATETIME);
    assertFieldType(bqSchema, "local_ts_micros", LegacySQLTypeName.DATETIME);
    assertFieldType(bqSchema, "local_ts_nanos", LegacySQLTypeName.DATETIME);
    assertFieldType(bqSchema, "time_micros", LegacySQLTypeName.TIME);

    // Verify actual values
    List<List<Object>> rows = readAllRows(bigQuery, table, "ts_micros");
    assertEquals(1, rows.size());
    List<Object> row = rows.get(0);

    // TIMESTAMP fields: getTimestampValue() returns epoch microseconds
    assertEquals(TIMESTAMP_MICROS, row.get(0), "ts_micros");
    assertEquals(TIMESTAMP_MICROS, row.get(1), "ts_nanos (truncated to micros)");

    // DATETIME fields: getTimestampValue() returns epoch microseconds interpreted as UTC
    assertEquals(TIMESTAMP_MILLIS * 1_000L, row.get(2), "local_ts_millis");
    assertEquals(TIMESTAMP_MICROS, row.get(3), "local_ts_micros");
    assertEquals(TIMESTAMP_MICROS, row.get(4), "local_ts_nanos (truncated to micros)");

    // TIME field: getStringValue() returns formatted string
    assertEquals("22:20:38.808123", row.get(5), "time_micros");
  }

  private void produceRecord(String topic, long id) {
    Struct key = new Struct(keySchema).put("id", id);
    Struct value = new Struct(valueSchema)
        .put("ts_micros", TIMESTAMP_MICROS)
        .put("ts_nanos", TIMESTAMP_NANOS)
        .put("local_ts_millis", TIMESTAMP_MILLIS)
        .put("local_ts_micros", TIMESTAMP_MICROS)
        .put("local_ts_nanos", TIMESTAMP_NANOS)
        .put("time_micros", TIME_MICROS);

    List<List<SchemaAndValue>> records = new ArrayList<>();
    List<SchemaAndValue> record = new ArrayList<>();
    record.add(new SchemaAndValue(keySchema, key));
    record.add(new SchemaAndValue(valueSchema, value));
    records.add(record);

    schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, records, topic);
  }

  private Schema getBigQuerySchema(String tableName) {
    Table table = bigQuery.getTable(dataset(), tableName);
    assertNotNull(table, "BigQuery table '" + tableName + "' was not created");
    return table.getDefinition().getSchema();
  }

  private void assertFieldType(Schema schema, String fieldName, LegacySQLTypeName expectedType) {
    Field field = schema.getFields().get(fieldName);
    assertNotNull(field, "Expected field '" + fieldName + "' not found in BigQuery schema");
    assertEquals(
        expectedType,
        field.getType(),
        "Field '" + fieldName + "' should be " + expectedType + " but was " + field.getType()
    );
  }

  private java.util.Map<String, String> connectorProps(String topic) {
    java.util.Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistry.schemaRegistryUrl());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
        schemaRegistry.schemaRegistryUrl());
    return props;
  }
}
