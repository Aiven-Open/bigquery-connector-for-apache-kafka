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
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.PrimaryKey;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableConstraints;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.SchemaRegistryTestUtils;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import io.confluent.connect.avro.AvroConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
class StorageWriteApiCdcBigQuerySinkConnectorIT extends BaseConnectorIT {

  private static final Logger logger =
      LoggerFactory.getLogger(StorageWriteApiCdcBigQuerySinkConnectorIT.class);

  private static final int TASKS_MAX = 1;

  private BigQuery bigQuery;

  @BeforeAll
  static void beforeAll() {
    startConnect();
  }

  @AfterAll
  static void afterAll() {
    stopConnect();
  }

  @BeforeEach
  void setup() {
    bigQuery = newBigQuery();
  }

  @AfterEach
  void close() {
    try {
      assertCluster().deleteConnector(connectorName());
    } catch (Exception e) {
      logger.debug("Ignoring failure to delete connector {} during cleanup", connectorName(), e);
    }
    if (bigQuery != null) {
      delete(bigQuery, tableName());
    }
    bigQuery = null;
  }

  private void createTableWithPrimaryKey(TableName tableName, String... keyColumns) {
    com.google.cloud.bigquery.Schema tableSchema =
        com.google.cloud.bigquery.Schema.of(
            Field.of("k1", StandardSQLTypeName.INT64), Field.of("f1", StandardSQLTypeName.STRING));
    createTableWithPrimaryKey(tableName, tableSchema, keyColumns);
  }

  private void createTableWithPrimaryKey(
      TableName tableName, com.google.cloud.bigquery.Schema tableSchema, String... keyColumns) {
    TableId tableId = TableNameUtils.tableId(tableName);

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
      logger.info("Table {} with primary key constraints created successfully", tableId);
      bigQuery.query(
          com.google.cloud.bigquery.QueryJobConfiguration.of(
              String.format(
                  "ALTER TABLE `%s`.`%s` SET OPTIONS (max_staleness = INTERVAL 0 MINUTE)",
                  tableName.getDataset(), tableName.getTable())));
      logger.info("Table {} max_staleness set to 0-0-0 successfully", tableId);
      Thread.sleep(30000);
      logger.info("Waited 30 seconds for metadata propagation");
    } catch (BigQueryException ex) {
      if (!ex.getError().getReason().equalsIgnoreCase("duplicate")) {
        throw new ConnectException("Failed to create table: ", ex);
      } else {
        logger.info("Table {} already exists", tableId);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Failed to set max_staleness on table: ", ex);
    }
  }

  @Test
  void testStorageWriteApiNativeCdc() throws Throwable {
    final String topic = topicName();
    final TableName tableName = tableName();
    assertCluster().kafka().createTopic(topic, TASKS_MAX);

    // Pre-create table with primary key 'k1'
    createTableWithPrimaryKey(tableName, "k1");

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
    assertCluster().configureConnector(connectorName(), props);
    waitForConnectorToStart(connectorName(), TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // Produce records:
    // 1. Insert record for key 1
    assertCluster()
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "original value", false));

    // 2. Insert record for key 2
    assertCluster()
        .kafka()
        .produce(
            topic, key(keyConverter, topic, 2L), value(valueConverter, topic, "other row", false));

    // 3. Update record for key 1
    assertCluster()
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "modified value", false));

    // 4. Delete record for key 1 (tombstone)
    assertCluster()
        .kafka()
        .produce(topic, key(keyConverter, topic, 1L), value(valueConverter, topic, null, true));

    // Wait for all 4 records to commit
    waitForCommittedRecords(connectorName(), topic, 4, TASKS_MAX);

    // Read back rows from BigQuery, sorting by k1 column, waiting for merge to happen
    org.apache.kafka.test.TestUtils.waitForCondition(
        () -> {
          try {
            return readAllRows(bigQuery, tableName, "k1").size() == 1;
          } catch (Exception e) {
            return false;
          }
        },
        60000,
        "Timed out waiting for CDC table to merge and show 1 row");

    List<List<Object>> allRows = readAllRows(bigQuery, tableName, "k1");

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
  void testStorageWriteApiCdcCompositeKey() throws Throwable {
    final String topic = topicName();
    final TableName tableName = tableName();
    assertCluster().kafka().createTopic(topic, TASKS_MAX);

    // Pre-create table with composite primary key 'k1' and 'k2'
    com.google.cloud.bigquery.Schema tableSchema =
        com.google.cloud.bigquery.Schema.of(
            Field.of("k1", StandardSQLTypeName.INT64),
            Field.of("k2", StandardSQLTypeName.STRING),
            Field.of("f1", StandardSQLTypeName.STRING));
    createTableWithPrimaryKey(tableName, tableSchema, "k1", "k2");

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
    assertCluster().configureConnector(connectorName(), props);
    waitForConnectorToStart(connectorName(), TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // 1. Insert record for key (1, "a") -> "original row 1"
    assertCluster()
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, "original row 1", false));

    // 2. Insert record for key (2, "b") -> "other row"
    assertCluster()
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 2L, "b"),
            value(valueConverter, topic, "other row", false));

    // 3. Update record for key (1, "a") -> "modified row 1"
    assertCluster()
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, "modified row 1", false));

    // 4. Delete record for key (1, "a") (Tombstone)
    assertCluster()
        .kafka()
        .produce(
            topic,
            compositeKey(keyConverter, topic, 1L, "a"),
            value(valueConverter, topic, null, true));

    // Wait for all 4 records to be committed
    waitForCommittedRecords(connectorName(), topic, 4, TASKS_MAX);

    // Read back rows from BigQuery, waiting for merge
    org.apache.kafka.test.TestUtils.waitForCondition(
        () -> {
          try {
            return readAllRows(bigQuery, tableName, "k1").size() == 1;
          } catch (Exception e) {
            return false;
          }
        },
        60000,
        "Timed out waiting for CDC table to merge and show 1 row");

    List<List<Object>> allRows = readAllRows(bigQuery, tableName, "k1");

    // The final result should contain only row 2, because key (1, "a") was updated then deleted.
    assertEquals(1, allRows.size());
    assertEquals(2L, allRows.get(0).get(0)); // k1
    assertEquals("b", allRows.get(0).get(1)); // k2
    assertEquals("other row", allRows.get(0).get(2)); // f1
  }

  @Test
  void testStorageWriteApiCdcDeleteDisabled() throws Throwable {
    final String topic = topicName();
    final TableName tableName = tableName();
    assertCluster().kafka().createTopic(topic, TASKS_MAX);

    // Pre-create table with primary key 'k1'
    createTableWithPrimaryKey(tableName, "k1");

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
    assertCluster().configureConnector(connectorName(), props);
    waitForConnectorToStart(connectorName(), TASKS_MAX);

    Converter keyConverter = converter(true);
    Converter valueConverter = converter(false);

    // 1. Insert record for key 1
    assertCluster()
        .kafka()
        .produce(
            topic,
            key(keyConverter, topic, 1L),
            value(valueConverter, topic, "original row 1", false));

    // 2. Produce tombstone for key 1 (should fail because delete is disabled)
    assertCluster().kafka().produce(topic, key(keyConverter, topic, 1L), null);

    // Wait for all 2 records to be committed (skipped records still have their offsets committed)
    waitForCommittedRecords(connectorName(), topic, 2, TASKS_MAX);

    // Read back rows from BigQuery. The row should still exist because delete was disabled.
    List<List<Object>> allRows = readAllRows(bigQuery, tableName, "k1");
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
  void testAvroHighThroughput() throws Exception {
    final String topic = topicName();
    final TableName tableName = tableName();
    final String connectorName = connectorName();

    final int tasksMax = 10;
    final long insertCount = 8_000_000L;
    final long updateCount = 1_000_000L;
    final long deleteCount = 1_000_000L;
    final long numRecords = insertCount + updateCount + deleteCount;
    final long expectedRows = insertCount - deleteCount;
    final int valueSize = 100;
    final int produceBatchSize = 200_000;

    // Pre-create destination table with primary key 'k1' and max_staleness = 0
    createTableWithPrimaryKey(tableName, "k1");

    // Create 10-partition topic
    assertCluster().kafka().createTopic(topic, tasksMax);

    SchemaRegistryTestUtils schemaRegistry =
        new SchemaRegistryTestUtils(assertCluster().kafka().bootstrapServers());
    schemaRegistry.start();
    try {
      String schemaRegistryUrl = schemaRegistry.schemaRegistryUrl();

      Converter avroKeyConverter = new AvroConverter();
      Converter avroValueConverter = new AvroConverter();
      avroKeyConverter.configure(
          Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), true);
      avroValueConverter.configure(
          Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

      Schema avroKeySchema = SchemaBuilder.struct().field("k1", Schema.INT64_SCHEMA).build();
      Schema avroValueSchema =
          SchemaBuilder.struct().optional().field("f1", Schema.STRING_SCHEMA).build();

      String initialPayload = String.join("", Collections.nCopies(valueSize, "A"));
      String updatedPayload = String.join("", Collections.nCopies(valueSize, "B"));
      SchemaAndValue initialValue =
          new SchemaAndValue(
              avroValueSchema, new Struct(avroValueSchema).put("f1", initialPayload));
      SchemaAndValue updatedValue =
          new SchemaAndValue(
              avroValueSchema, new Struct(avroValueSchema).put("f1", updatedPayload));

      // 1. Produce 8M inserts (k1 = 0 .. 7,999,999)
      produceAvroCdcRecordsInBatches(
          schemaRegistry,
          avroKeyConverter,
          avroValueConverter,
          topic,
          avroKeySchema,
          0L,
          insertCount,
          initialValue,
          produceBatchSize);

      // 2. Produce 1M updates (k1 = 0 .. 999,999)
      produceAvroCdcRecordsInBatches(
          schemaRegistry,
          avroKeyConverter,
          avroValueConverter,
          topic,
          avroKeySchema,
          0L,
          updateCount,
          updatedValue,
          produceBatchSize);

      // 3. Produce 1M tombstone deletes (k1 = 1,000,000 .. 1,999,999)
      produceAvroCdcRecordsInBatches(
          schemaRegistry,
          avroKeyConverter,
          avroValueConverter,
          topic,
          avroKeySchema,
          updateCount,
          deleteCount,
          null,
          produceBatchSize);

      Map<String, String> props = baseConnectorProps(tasksMax);
      props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);
      props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
      props.put(
          BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
      props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
      props.put(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, "false");

      // Enable native Storage Write API CDC
      props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
      props.put(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, "true");
      props.put(BigQuerySinkConfig.DELETE_ENABLED_CONFIG, "true");

      props.put(KEY_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
      props.put(
          ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
          schemaRegistryUrl);
      props.put(VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
      props.put(
          ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG + "." + SCHEMA_REGISTRY_URL_CONFIG,
          schemaRegistryUrl);

      // The Storage Write API allows for 10MB per write; fetch ~9MB per poll so JSON expansion
      // exercises multi-batch splitting across concurrent tasks.
      props.put(
          CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + FETCH_MAX_BYTES_CONFIG,
          Integer.toString(9 * 1024 * 1024));
      props.put(
          CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_PARTITION_FETCH_BYTES_CONFIG,
          Integer.toString(Integer.MAX_VALUE));
      props.put(
          CONNECTOR_CLIENT_CONSUMER_OVERRIDES_PREFIX + MAX_POLL_RECORDS_CONFIG,
          Long.toString(numRecords));

      assertCluster().configureConnector(connectorName, props);
      waitForConnectorToStart(connectorName, tasksMax);

      waitForCommittedRecords(
          connectorName,
          Collections.singleton(topic),
          numRecords,
          tasksMax,
          TimeUnit.MINUTES.toMillis(15),
          TimeUnit.SECONDS.toMillis(10));

      final AtomicLong numRows = new AtomicLong();
      TestUtils.waitForCondition(
          () -> {
            numRows.set(countRows(bigQuery, tableName));
            return numRows.get() == expectedRows;
          },
          TimeUnit.MINUTES.toMillis(10),
          TimeUnit.SECONDS.toMillis(10),
          () ->
              "Table should contain "
                  + expectedRows
                  + " merged rows, but has "
                  + numRows.get()
                  + " instead");

      // Give the system an extra minute to ensure no delayed or duplicate rows appear.
      Thread.sleep(TimeUnit.MINUTES.toMillis(1));
      numRows.set(countRows(bigQuery, tableName));
      assertEquals(
          expectedRows,
          numRows.get(),
          () ->
              "Table should contain "
                  + expectedRows
                  + " merged rows after settle period, but has "
                  + numRows.get()
                  + " instead");

      String breakdownSql =
          String.format(
              "SELECT COUNT(DISTINCT k1), "
                  + "COUNTIF(k1 < @updateEnd AND f1 = @updatedPayload), "
                  + "COUNTIF(k1 >= @updateEnd AND k1 < @deleteEnd), "
                  + "COUNTIF(k1 >= @deleteEnd AND f1 = @initialPayload) "
                  + "FROM `%s`.`%s`",
              tableName.getDataset(), tableName.getTable());
      TableResult breakdownResult =
          bigQuery.query(
              QueryJobConfiguration.newBuilder(breakdownSql)
                  .addNamedParameter("updateEnd", QueryParameterValue.int64(updateCount))
                  .addNamedParameter(
                      "deleteEnd", QueryParameterValue.int64(updateCount + deleteCount))
                  .addNamedParameter("updatedPayload", QueryParameterValue.string(updatedPayload))
                  .addNamedParameter("initialPayload", QueryParameterValue.string(initialPayload))
                  .build());
      FieldValueList breakdownRow = breakdownResult.iterateAll().iterator().next();
      assertEquals(expectedRows, breakdownRow.get(0).getLongValue(), "Distinct key count mismatch");
      assertEquals(updateCount, breakdownRow.get(1).getLongValue(), "Updated row count mismatch");
      assertEquals(0L, breakdownRow.get(2).getLongValue(), "Deleted rows should not be present");
      assertEquals(
          insertCount - updateCount - deleteCount,
          breakdownRow.get(3).getLongValue(),
          "Unmodified inserted row count mismatch");
    } finally {
      schemaRegistry.stop();
    }
  }

  private void produceAvroCdcRecordsInBatches(
      SchemaRegistryTestUtils schemaRegistry,
      Converter keyConverter,
      Converter valueConverter,
      String topic,
      Schema keySchema,
      long startKey,
      long totalRecords,
      SchemaAndValue valueSchemaAndValue,
      int batchSize) {
    long produced = 0;
    while (produced < totalRecords) {
      int currentBatchSize = (int) Math.min(batchSize, totalRecords - produced);
      List<List<SchemaAndValue>> batch = new ArrayList<>(currentBatchSize);
      for (int i = 0; i < currentBatchSize; i++) {
        long keyId = startKey + produced + i;
        SchemaAndValue keySchemaAndValue =
            new SchemaAndValue(keySchema, new Struct(keySchema).put("k1", keyId));
        batch.add(Arrays.asList(keySchemaAndValue, valueSchemaAndValue));
      }
      schemaRegistry.produceRecordsWithKey(keyConverter, valueConverter, batch, topic);
      produced += currentBatchSize;
    }
  }
}
