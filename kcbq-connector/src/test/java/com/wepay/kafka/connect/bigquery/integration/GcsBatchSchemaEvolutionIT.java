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
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import io.confluent.connect.avro.AvroConverter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class GcsBatchSchemaEvolutionIT extends BaseConnectorIT {
  
  private static final int TASKS_MAX = 1;
  private static final Duration LOAD_TIMEOUT = Duration.ofMinutes(2);

  private BigQuery bigQuery;
  private SchemaRegistryTestUtils schemaRegistry;
  private String schemaRegistryUrl;
  private Converter keyConverter;
  private Converter valueConverter;
  private Schema keySchema;
  private Schema valueSchemaV1;
  private Schema valueSchemaV2;
  private String topic;
  private TableName tableName;
  private String connectorName;

  @BeforeAll
  static void beforeAll() {
    startConnect();
  }

  @AfterAll
  static void afterAll() {
    stopConnect();
  }

  @BeforeEach
  void setup() throws Exception {
    bigQuery = newBigQuery();

    schemaRegistry = new SchemaRegistryTestUtils(assertCluster().kafka().bootstrapServers());
    schemaRegistry.start();
    schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

    initialiseSchemas();
    initialiseConverters();

    topic = topicName();
    tableName = tableName();
    connectorName = connectorName();
    assertCluster().configureConnector(connectorName, connectorProps());
    assertCluster().kafka().createTopic(topic);

    //createBucket();
    createInitialTable();
    waitForConnectorToStart(connectorName, TASKS_MAX);

  }

  @AfterEach
  void tearDown() throws Exception {
      if (schemaRegistry != null) {
        schemaRegistry.stop();
      }
      if (bigQuery != null) {
        delete(bigQuery, tableName());
      }
      clearBucket();
  }

  @Test
  void testSchemaEvolutionAcrossBatchLoads() throws Exception {

    schemaRegistry.produceRecordsWithKey(
        keyConverter, valueConverter, Collections.singletonList(recordV1(1L, "snacks")), topic);

    waitForCommittedRecords(connectorName, topic, 1, TASKS_MAX);
    waitForRowCount(1L);

    schemaRegistry.produceRecordsWithKey(
        keyConverter, valueConverter, Collections.singletonList(recordV2(2L, null, "john")), topic);

    waitForCommittedRecords(connectorName, topic, 2, TASKS_MAX);
    waitForRowCount(2L);

    Table destinationTable = bigQuery.getTable(TableNameUtils.tableId(tableName));
    com.google.cloud.bigquery.Schema destinationSchema =
        destinationTable.getDefinition().getSchema();
    Field categoryField = destinationSchema.getFields().get("category");
    assertNotNull(categoryField, "category field should exist after load");
    assertEquals(Field.Mode.NULLABLE, categoryField.getMode());
    Field usernameField = destinationSchema.getFields().get("username");
    assertNotNull(usernameField, "username field should be created");
    assertEquals(Field.Mode.NULLABLE, usernameField.getMode());

    List<List<Object>> rows = readAllRows(bigQuery, tableName, "id");
    assertEquals(Arrays.asList(1L, "snacks", null), rows.get(0));
    assertEquals(Arrays.asList(2L, null, "john"), rows.get(1));
  }

  private Map<String, String> connectorProps() {
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(TOPICS_CONFIG, topic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, "true");
    props.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, "true");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, topic + "," + tableName);
    props.put(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG, "5");
    props.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, gcsBucket());
    props.put(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG, gcsFolder());
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    return props;
  }

  private void initialiseSchemas() {
    keySchema =
        SchemaBuilder.struct()
            .name("com.wepay.kafka.connect.bigquery.integration.Key")
            .field("id", Schema.INT64_SCHEMA)
            .build();

    valueSchemaV1 =
        SchemaBuilder.struct()
            .name("com.wepay.kafka.connect.bigquery.integration.ValueV1")
            .field("id", Schema.INT64_SCHEMA)
            .field("category", Schema.STRING_SCHEMA)
            .build();

    valueSchemaV2 =
        SchemaBuilder.struct()
            .name("com.wepay.kafka.connect.bigquery.integration.ValueV2")
            .field("id", Schema.INT64_SCHEMA)
            .field("category", SchemaBuilder.string().optional().build())
            .field("username", SchemaBuilder.string().optional().build())
            .build();
  }

  private void initialiseConverters() {
    keyConverter = new AvroConverter();
    valueConverter = new AvroConverter();
    Map<String, Object> keyConfig = new HashMap<>();
    keyConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    keyConverter.configure(keyConfig, true);
    Map<String, Object> valueConfig = new HashMap<>();
    valueConfig.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    valueConverter.configure(valueConfig, false);
  }

  private List<SchemaAndValue> recordV1(long id, String category) {
    Struct key = new Struct(keySchema).put("id", id);
    Struct value = new Struct(valueSchemaV1).put("id", id).put("category", category);
    List<SchemaAndValue> record = new ArrayList<>(2);
    record.add(new SchemaAndValue(keySchema, key));
    record.add(new SchemaAndValue(valueSchemaV1, value));
    return record;
  }

  private List<SchemaAndValue> recordV2(long id, String category, String username) {
    Struct key = new Struct(keySchema).put("id", id);
    Struct value =
        new Struct(valueSchemaV2).put("id", id).put("category", category).put("username", username);
    List<SchemaAndValue> record = new ArrayList<>(2);
    record.add(new SchemaAndValue(keySchema, key));
    record.add(new SchemaAndValue(valueSchemaV2, value));
    return record;
  }

  private void createInitialTable() {
    com.google.cloud.bigquery.Schema schema =
        com.google.cloud.bigquery.Schema.of(
            Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("category", LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build());
    BigQueryTestUtils.createPartitionedTable(bigQuery, tableName, schema);
  }

  private void waitForRowCount(long expected) throws InterruptedException {
    waitForCondition(
        () -> {
          try {
            return countRows(bigQuery, tableName) >= expected;
          } catch (Exception e) {
            return false;
          }
        },
        LOAD_TIMEOUT.toMillis(),
        "Timed out waiting for " + expected + " rows");
  }
}
