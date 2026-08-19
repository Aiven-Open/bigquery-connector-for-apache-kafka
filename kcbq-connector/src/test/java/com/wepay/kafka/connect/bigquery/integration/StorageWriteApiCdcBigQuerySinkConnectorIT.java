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

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
public class StorageWriteApiCdcBigQuerySinkConnectorIT extends BaseConnectorIT {

  private static final Logger logger =
      LoggerFactory.getLogger(StorageWriteApiCdcBigQuerySinkConnectorIT.class);

  private static final int TASKS_MAX = 1;

  private String connectorName;
  private BigQuery bigQuery;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    String testMethod =
        testInfo
            .getTestMethod()
            .map(Method::getName)
            .orElseThrow(() -> new AssertionError("Test method not found"));
    connectorName = "kcbq-cdc-sink-connector-" + testMethod;
    bigQuery = newBigQuery();
    startConnect();
  }

  @AfterEach
  public void close() {
    bigQuery = null;
    stopConnect();
  }

  private void createTableWithPrimaryKey(String table, String... keyColumns) {
    com.google.cloud.bigquery.Schema tableSchema =
        com.google.cloud.bigquery.Schema.of(
            Field.of("k1", StandardSQLTypeName.INT64), Field.of("f1", StandardSQLTypeName.STRING));
    createTableWithPrimaryKey(table, tableSchema, keyColumns);
  }

  private void createTableWithPrimaryKey(
      String table, com.google.cloud.bigquery.Schema tableSchema, String... keyColumns) {
    TableId tableId = TableId.of(dataset(), table);

    PrimaryKey primaryKey = PrimaryKey.newBuilder().setColumns(Arrays.asList(keyColumns)).build();
    TableConstraints constraints = TableConstraints.newBuilder().setPrimaryKey(primaryKey).build();

    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(tableSchema)
            .setTableConstraints(constraints)
            .build();

    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    try {
      bigQuery.create(tableInfo);
      logger.info("Table {} with primary key constraints created successfully", table);
      bigQuery.query(
          com.google.cloud.bigquery.QueryJobConfiguration.of(
              String.format(
                  "ALTER TABLE `%s`.`%s` SET OPTIONS (max_staleness = INTERVAL 0 MINUTE)",
                  dataset(), table)));
      logger.info("Table {} max_staleness set to 0-0-0 successfully", table);
      Thread.sleep(60000);
      logger.info("Waited 60 seconds for metadata propagation");
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate")) {
        throw new ConnectException("Failed to create table: ", ex);
      } else {
        logger.info("Table {} already exists", table);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Failed to set max_staleness on table: ", ex);
    }
  }

  @Test
  public void testStorageWriteApiNativeCdc() throws Throwable {
    final String topic = suffixedTableOrTopic("test-storage-write-api-cdc");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    // Pre-create table with primary key 'k1'
    createTableWithPrimaryKey(table, "k1");

    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false"); // We pre-created it
    props.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");

    // Enable native CDC modes
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    props.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");

    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // start the sink connector
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Produce records:
    // 1. Insert record for key 1
    connect
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "original value", false));

    // 2. Insert record for key 2
    connect
        .kafka()
        .produce(
            topic, key(keyConverter, topic, 2L), value(valueConverter, topic, "other row", false));

    // 3. Update record for key 1
    connect
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "modified value", false));

    // 4. Delete record for key 1 (tombstone)
    connect
        .kafka()
        .produce(topic, key(keyConverter, topic, 1L), value(valueConverter, topic, null, true));

    // Wait for all 4 records to commit
    waitForCommittedRecords(connectorName, topic, 4, TASKS_MAX);

    // Read back rows from BigQuery, sorting by k1 column, waiting for merge to happen
    org.apache.kafka.test.TestUtils.waitForCondition(
        () -> {
          try {
            return readAllRows(bigQuery, table, "k1").size() == 1;
          } catch (Exception e) {
            return false;
          }
        },
        60000,
        "Timed out waiting for CDC table to merge and show 1 row");

    List<List<Object>> allRows = readAllRows(bigQuery, table, "k1");

    // The final result should contain only row 2, because key 1 was updated then deleted.
    // The query returns fields in order of table schema: k1, f1, _CHANGE_TYPE,
    // _CHANGE_SEQUENCE_NUMBER
    // Since BigQuery CDC merges base table with changes, it should return 1 row.
    // Note: BQ CDC columns (_CHANGE_TYPE, _CHANGE_SEQUENCE_NUMBER) are system columns and we don't
    // assert their exact values here if we only want the materialized state,
    // but they will be returned by "SELECT *".
    // We check the first two columns (k1 and f1).
    assertEquals(1, allRows.size());
    assertEquals(2L, allRows.get(0).get(0)); // k1
    assertEquals("other row", allRows.get(0).get(1)); // f1
  }

  @Test
  public void testStorageWriteApiCdcCompositeKey() throws Throwable {
    final String topic = suffixedTableOrTopic("test-storage-write-api-cdc-composite");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    // Pre-create table with composite primary key 'k1' and 'k2'
    com.google.cloud.bigquery.Schema tableSchema =
        com.google.cloud.bigquery.Schema.of(
            Field.of("k1", StandardSQLTypeName.INT64),
            Field.of("k2", StandardSQLTypeName.STRING),
            Field.of("f1", StandardSQLTypeName.STRING));
    createTableWithPrimaryKey(table, tableSchema, "k1", "k2");

    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    props.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");

    // Enable native CDC modes
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    props.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");

    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // start the sink connector
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // 1. Insert record for key (1, "a") -> "original row 1"
    connect
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, "original row 1", false));

    // 2. Insert record for key (2, "b") -> "other row"
    connect
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 2L, "b"),
            value(valueConverter, topic, "other row", false));

    // 3. Update record for key (1, "a") -> "modified row 1"
    connect
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, "modified row 1", false));

    // 4. Delete record for key (1, "a") (Tombstone)
    connect
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, null, true));

    // Wait for all 4 records to be committed
    waitForCommittedRecords(connectorName, topic, 4, TASKS_MAX);

    // Read back rows from BigQuery, waiting for merge
    org.apache.kafka.test.TestUtils.waitForCondition(
        () -> {
          try {
            return readAllRows(bigQuery, table, "k1").size() == 1;
          } catch (Exception e) {
            return false;
          }
        },
        60000,
        "Timed out waiting for CDC table to merge and show 1 row");

    List<List<Object>> allRows = readAllRows(bigQuery, table, "k1");

    // The final result should contain only row 2, because key (1, "a") was updated then deleted.
    assertEquals(1, allRows.size());
    assertEquals(2L, allRows.get(0).get(0)); // k1
    assertEquals("b", allRows.get(0).get(1)); // k2
    assertEquals("other row", allRows.get(0).get(2)); // f1
  }

  private void waitForTaskToFail(String connectorName) throws InterruptedException {
    org.apache.kafka.test.TestUtils.waitForCondition(
        () -> {
          try {
            org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo info =
                connect.connectorStatus(connectorName);
            return info != null && info.tasks().stream().anyMatch(s -> s.state().equals("FAILED"));
          } catch (Exception e) {
            return false;
          }
        },
        30000,
        "Timed out waiting for connector task to fail");
  }

  @Test
  public void testStorageWriteApiCdcDeleteDisabled() throws Throwable {
    final String topic = suffixedTableOrTopic("test-storage-write-api-cdc-del-disabled");
    connect.kafka().createTopic(topic, TASKS_MAX);

    final String table = sanitizedTable(topic);
    TableClearer.clearTables(bigQuery, dataset(), table);

    // Pre-create table with primary key 'k1'
    createTableWithPrimaryKey(table, "k1");

    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    props.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");

    // Enable native CDC modes, but DISABLE delete
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
    props.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "false"); // DISABLE DELETE

    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    // start the sink connector
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // 1. Insert record for key 1
    connect
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "original row 1", false));

    // 2. Produce tombstone for key 1 (should fail because delete is disabled)
    connect.kafka().produce(topic, key(keyConverter, topic, 1L), null);

    // Wait for all 2 records to be committed (skipped records still have their offsets committed)
    waitForCommittedRecords(connectorName, topic, 2, TASKS_MAX);

    // Read back rows from BigQuery. The row should still exist because delete was disabled.
    List<List<Object>> allRows = readAllRows(bigQuery, table, "k1");
    assertEquals(1, allRows.size());
    assertEquals(1L, allRows.get(0).get(0));
    assertEquals("original row 1", allRows.get(0).get(1));
  }

  private Converter converter(boolean isKey) {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, isKey);
    return result;
  }

  private String key(Converter converter, String topic, long id) {
    final Schema schema = SchemaBuilder.struct().field("k1", Schema.INT64_SCHEMA).build();

    final Struct struct = new Struct(schema).put("k1", id);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String value(Converter converter, String topic, String val, boolean tombstone) {
    final Schema schema =
        SchemaBuilder.struct().optional().field("f1", Schema.STRING_SCHEMA).build();

    if (tombstone) {
      return new String(converter.fromConnectData(topic, schema, null));
    }

    final Struct struct = new Struct(schema).put("f1", val);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  private String compositeKey(Converter converter, String topic, long id1, String id2) {
    final Schema schema =
        SchemaBuilder.struct()
            .field("k1", Schema.INT64_SCHEMA)
            .field("k2", Schema.STRING_SCHEMA)
            .build();

    final Struct struct = new Struct(schema).put("k1", id1).put("k2", id2);

    return new String(converter.fromConnectData(topic, schema, struct));
  }

  @Test
  public void testPrintStreamSchema() throws Exception {
    String tableName = sanitizedTable(suffixedTableOrTopic("test-storage-write-api-cdc"));
    com.google.cloud.bigquery.Table table = bigQuery.getTable(TableId.of(dataset(), tableName));
    if (table != null) {
      System.out.println("TABLE SCHEMA: " + table.getDefinition().getSchema());
      System.out.println("TABLE DEFINITION: " + table.getDefinition());
    } else {
      System.out.println("TABLE DOES NOT EXIST");
      return;
    }

    String streamName = "projects/" + project() + "/datasets/" + dataset() + "/tables/" + tableName;
    com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings settings =
        new com.wepay.kafka.connect.bigquery.GcpClientBuilder.BigQueryWriteSettingsBuilder()
            .withKey(keyFile())
            .withKeySource(
                com.wepay.kafka.connect.bigquery.GcpClientBuilder.KeySource.valueOf(keySource()))
            .withProject(project())
            .withWriterApi(true)
            .build();
    try (com.google.cloud.bigquery.storage.v1.BigQueryWriteClient client =
        com.google.cloud.bigquery.storage.v1.BigQueryWriteClient.create(settings)) {
      com.google.cloud.bigquery.storage.v1.WriteStream writeStream =
          client.getWriteStream(
              com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest.newBuilder()
                  .setName(streamName + "/streams/_default")
                  .build());
      System.out.println("STREAM: " + writeStream);
      System.out.println("STREAM FIELDS COUNT: " + writeStream.getTableSchema().getFieldsCount());
      logger.info("STREAM: " + writeStream);
      logger.info("STREAM FIELDS COUNT: " + writeStream.getTableSchema().getFieldsCount());
    }
  }
}
