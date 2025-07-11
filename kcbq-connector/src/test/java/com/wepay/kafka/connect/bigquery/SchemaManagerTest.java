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

package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

public class SchemaManagerTest {

  private String testTableName = "testTable";
  private String testDatasetName = "testDataset";
  private String testDoc = "test doc";
  private TableId tableId = TableId.of(testDatasetName, testTableName);

  private SchemaRetriever mockSchemaRetriever;
  private SchemaConverter<com.google.cloud.bigquery.Schema> mockSchemaConverter;
  private BigQuery mockBigQuery;
  private Schema mockKafkaSchema;
  private com.google.cloud.bigquery.Schema fakeBigQuerySchema;

  @BeforeEach
  public void before() {
    mockSchemaRetriever = mock(SchemaRetriever.class);
    mockSchemaConverter =
        (SchemaConverter<com.google.cloud.bigquery.Schema>) mock(SchemaConverter.class);
    mockBigQuery = mock(BigQuery.class);
    mockKafkaSchema = mock(Schema.class);
    fakeBigQuerySchema = com.google.cloud.bigquery.Schema.of(
        Field.of("mock field", LegacySQLTypeName.STRING));
  }

  @Test
  public void testBQTableDescription() {
    Optional<String> kafkaKeyFieldName = Optional.of("kafkaKey");
    Optional<String> kafkaDataFieldName = Optional.of("kafkaData");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, kafkaKeyFieldName, kafkaDataFieldName,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    assertNull(
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField(),
        "Timestamp partition field name is not null"
    );
    assertNull(
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getExpirationMs(),
        "Partition expiration is not null"
    );
  }

  @Test
  public void testTimestampPartitionSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNotNull(definition.getTimePartitioning());
    assertEquals(TimePartitioning.Type.DAY, definition.getTimePartitioning().getType());
    assertEquals(
        testField.get(),
        definition.getTimePartitioning().getField(),
        "The field name does not match the field name of time partition"
    );
    assertNull(
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getExpirationMs(),
        "Partition expiration is not null"
    );
  }

  @Test
  public void testAlternativeTimestampPartitionType() {
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.of(TimePartitioning.Type.HOUR), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNotNull(definition.getTimePartitioning());
    assertEquals(TimePartitioning.Type.HOUR, definition.getTimePartitioning().getType());
  }

  @Test
  public void testNoTimestampPartitionType() {
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty(), Optional.empty(), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNull(definition.getTimePartitioning());
  }

  @Test
  public void testUpdateTimestampPartitionNull() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, false);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    assertNull(
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning(),
        "The time partitioning object should be null"
    );
  }

  @Test
  public void testUpdateTimestampPartitionNotSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), testField,
        Optional.empty(), Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNotNull(definition.getTimePartitioning());
    assertEquals(
        testField.get(),
        definition.getTimePartitioning().getField(),
        "The field name does not match the field name of time partition"
    );

    Optional<String> updateField = Optional.of("testUpdateField");
    schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), updateField, Optional.empty(), Optional.empty(),
        Optional.of(TimePartitioning.Type.DAY), false);

    tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, false);
    definition = tableInfo.getDefinition();
    assertNull(
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning(),
        "The time partitioning object should be null"
    );
  }

  @Test
  public void testPartitionExpirationSetWithoutFieldName() {
    Optional<Long> testExpirationMs = Optional.of(86400000L);
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), Optional.empty(),
        testExpirationMs, Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition tableDefinition = tableInfo.getDefinition();
    assertEquals(
        testExpirationMs.get(),
        tableDefinition.getTimePartitioning().getExpirationMs(),
        "The partition expiration does not match the expiration in ms"
    );
    assertNull(
        tableDefinition.getTimePartitioning().getField(),
        "Timestamp partition field name is not null"
    );
  }

  @Test
  public void testPartitionExpirationSetWithFieldName() {
    Optional<Long> testExpirationMs = Optional.of(86400000L);
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), testField,
        testExpirationMs, Optional.empty(), Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition tableDefinition = (StandardTableDefinition) tableInfo.getDefinition();
    assertEquals(
        testExpirationMs.get(),
        tableDefinition.getTimePartitioning().getExpirationMs(),
        "The partition expiration does not match the expiration in ms"
    );
    assertEquals(
        testField.get(),
        tableDefinition.getTimePartitioning().getField(),
        "The field name does not match the field name of time partition"
    );
  }

  @Test
  public void testClusteringPartitionSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNotNull(definition.getClustering());
    assertEquals(
        testField.get(),
        definition.getClustering().getFields(),
        "The field name does not match the field name of time partition"
    );
  }

  @Test
  public void testUpdateClusteringPartitionNull() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, false);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNull(
        definition.getClustering(),
        "The clustering object should be null"
    );
  }

  @Test
  public void testUpdateClusteringPartitionNotSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), testField, Optional.of(TimePartitioning.Type.DAY), false);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, true);

    assertEquals(
        testDoc,
        tableInfo.getDescription(),
        "Kafka doc does not match BigQuery table description"
    );
    StandardTableDefinition definition = tableInfo.getDefinition();
    assertNotNull(definition.getClustering());
    assertEquals(
        testField.get(),
        definition.getClustering().getFields(),
        "The field name should not match the field name of time partition"
    );

    Optional<List<String>> updateTestField = Optional.of(Arrays.asList("column3", "column4"));
    schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName,
        Optional.empty(), updateTestField, Optional.of(TimePartitioning.Type.DAY), false);

    tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, Collections.emptyList(), testDoc, false);
    definition = tableInfo.getDefinition();
    assertNull(
        definition.getClustering(),
        "The clustering object should be null"
    );
  }

  @Test
  public void testSuccessfulUpdateWithOnlyRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema relaxedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, relaxedSchema, relaxedSchema);
  }

  @Test
  public void testDisallowedUpdateWithOnlyRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema relaxedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(schemaManager, existingSchema, relaxedSchema, null)
    );
  }

  @Test
  public void testSuccessfulUpdateWithOnlyNewFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expandedSchema);
  }

  @Test
  public void testDisallowedUpdateWithOnlyNewFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, false);

    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null)
    );
  }

  @Test
  public void testDisallowedUpdateWithOnlyNewRequiredFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null)
    );
  }

  @Test
  public void testSuccessfulUpdateWithNewAndRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedAndRelaxedSchema = com.google.cloud.bigquery.Schema.of(
        // Relax an existing field from required to nullable
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        // Add a new nullable field
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
        // Add a new required field (that should be relaxed to nullable automatically)
        Field.newBuilder("f3", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f3", LegacySQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false);

    testGetAndValidateProposedSchema
        (schemaManager, existingSchema, expandedAndRelaxedSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUnionizedUpdateWithNewAndRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema disjointSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, disjointSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUnionizedUpdateWithNewRepeatedField() {
    com.google.cloud.bigquery.Schema reducedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    // Unionization should work symmetrically, so test both cases of reduced/expanded as the current/new schemas
    testGetAndValidateProposedSchema(schemaManager, reducedSchema, expandedSchema, expectedSchema);
    testGetAndValidateProposedSchema(schemaManager, expandedSchema, reducedSchema, expectedSchema);
  }

  @Test
  public void testSuccessfulUpdateWithNewRepeatedField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REPEATED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expectedSchema);
  }

  @Test
  public void testDisallowedUnionizedUpdateWithNewField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, true);

    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null)
    );
  }

  @Test
  public void testDisallowedUnionizedUpdateWithRelaxedField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, true);

    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null)
    );
  }

  @Test
  public void testUnionizedUpdateWithMultipleSchemas() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema firstNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );
    com.google.cloud.bigquery.Schema secondNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );
    com.google.cloud.bigquery.Schema thirdNewSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );
    List<com.google.cloud.bigquery.Schema> newSchemas =
        Arrays.asList(firstNewSchema, secondNewSchema, thirdNewSchema);

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, newSchemas, expectedSchema);
  }

  @Test
  public void FieldsWithUnspecifiedModeShouldNotCauseNpe() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).build()
    );

    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, expectedSchema);
  }

  @Test
  public void testFieldNamesSanitizedNoExistingSchema() {
    BigQuerySchemaConverter converter = new BigQuerySchemaConverter(false, true);

    Schema kafkaSchema = SchemaBuilder.struct()
        .field("f 1", Schema.BOOLEAN_SCHEMA)
        .field("f 2", Schema.INT32_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f_1", LegacySQLTypeName.BOOLEAN).setMode(Mode.REQUIRED).build(),
        Field.newBuilder("f_2", LegacySQLTypeName.INTEGER).setMode(Mode.REQUIRED).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, false, false, true, converter);
    testGetAndValidateProposedSchema(schemaManager, null,
        null, expectedSchema,
        Collections.singletonList(recordWithValueSchema(kafkaSchema)));
  }

  @Test
  public void testFieldNameSanitizedNewFields() {
    BigQuerySchemaConverter converter = new BigQuerySchemaConverter(false, true);
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.REQUIRED).build()
    );
    Schema kafkaSchema = SchemaBuilder.struct()
        .field("f1", Schema.BOOLEAN_SCHEMA)
        .field("f 1", Schema.BOOLEAN_SCHEMA)
        .field("f 2", Schema.INT32_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.REQUIRED).build(),
        Field.newBuilder("f_1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build(),
        Field.newBuilder("f_2", LegacySQLTypeName.INTEGER).setMode(Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false, true, converter);
    testGetAndValidateProposedSchema(schemaManager, existingSchema,
        null, expectedSchema,
        Collections.singletonList(recordWithValueSchema(kafkaSchema)));
  }

  @Test
  public void testFieldNamesSanitizedUnionizedFields() {
    BigQuerySchemaConverter converter = new BigQuerySchemaConverter(false, true);
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.REQUIRED).build()
    );
    Schema kafkaSchema = SchemaBuilder.struct()
        .field("f 1", Schema.BOOLEAN_SCHEMA)
        .field("f 2", Schema.INT32_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build(),
        Field.newBuilder("f_1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build(),
        Field.newBuilder("f_2", LegacySQLTypeName.INTEGER).setMode(Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, true, true, converter);
    testGetAndValidateProposedSchema(schemaManager, existingSchema,
        null, expectedSchema,
        Collections.singletonList(recordWithValueSchema(kafkaSchema)));
  }

  @Test
  public void testFieldNamesSanitizedFieldRelaxation() {
    BigQuerySchemaConverter converter = new BigQuerySchemaConverter(false, true);
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f_1", LegacySQLTypeName.BOOLEAN).setMode(Mode.REQUIRED).build()
    );
    Schema kafkaSchema = SchemaBuilder.struct()
        .field("f 1", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema expectedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f_1", LegacySQLTypeName.BOOLEAN).setMode(Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, true, false, true, converter);
    testGetAndValidateProposedSchema(schemaManager, existingSchema,
        null, expectedSchema,
        Collections.singletonList(recordWithValueSchema(kafkaSchema)));
  }

  @Test
  public void testUpdateWithOnlyTombstoneRecordsAndExistingSchema() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);
    List<SinkRecord> incomingSinkRecords = Collections.nCopies(2, recordWithValueSchema(null));
    // Tombstone records are skipped, and existing schema is reused.
    testGetAndValidateProposedSchema(schemaManager, existingSchema,
        Collections.singletonList(existingSchema), existingSchema, incomingSinkRecords);
  }

  @Test
  public void testUpdateWithOnlyTombstoneRecordsNoExistingSchema() {
    SchemaManager schemaManager = createSchemaManager(true, false, false);
    List<SinkRecord> incomingSinkRecords = Collections.nCopies(2, recordWithValueSchema(null));
    assertThrows(
        BigQueryConnectException.class,
        () -> testGetAndValidateProposedSchema(
            schemaManager, null, Collections.singletonList(null), null, incomingSinkRecords)
    );
  }

  @Test
  public void testUpdateWithRegularAndTombstoneRecords() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);
    // Put tombstone at the end of the batch.
    List<SinkRecord> incomingSinkRecords = ImmutableList.of(
        recordWithValueSchema(mockKafkaSchema), recordWithValueSchema(null));
    // Tombstone record is skipped when converting schema.
    testGetAndValidateProposedSchema(schemaManager, existingSchema,
        Collections.singletonList(expandedSchema), expandedSchema, incomingSinkRecords);
  }

  @Test
  public void testGetUnionizedTableDescriptionFromTombstoneRecord() {
    SchemaManager schemaManager = createSchemaManager(false, true, true);
    SinkRecord tombstone = recordWithValueSchema(null);
    List<SinkRecord> incomingSinkRecords = ImmutableList.of(tombstone);
    assertNull(schemaManager.getUnionizedTableDescription(incomingSinkRecords));
  }

  @Test
  public void testGetUnionizedTableDescriptionFromRegularAndNullRecords() {
    SchemaManager schemaManager = createSchemaManager(false, true, true).forIntermediateTables();
    List<SinkRecord> incomingSinkRecords = ImmutableList.of(
        recordWithValueSchema(mockKafkaSchema), recordWithValueSchema(null));
    when(mockKafkaSchema.doc()).thenReturn(testDoc);
    assertNotNull(schemaManager.getUnionizedTableDescription(incomingSinkRecords));
  }

  private SchemaManager createSchemaManager(
      boolean allowNewFields, boolean allowFieldRelaxation, boolean allowUnionization, boolean sanitizeFieldNames, SchemaConverter<com.google.cloud.bigquery.Schema> converter) {
    return new SchemaManager(new IdentitySchemaRetriever(), converter, mockBigQuery,
        allowNewFields, allowFieldRelaxation, allowUnionization, sanitizeFieldNames,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.of(TimePartitioning.Type.DAY), false);
  }

  private SchemaManager createSchemaManager(
      boolean allowNewFields, boolean allowFieldRelaxation, boolean allowUnionization) {
    return new SchemaManager(new IdentitySchemaRetriever(), mockSchemaConverter, mockBigQuery,
        allowNewFields, allowFieldRelaxation, allowUnionization, false,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.of(TimePartitioning.Type.DAY), false);
  }

  private void testGetAndValidateProposedSchema(
      SchemaManager schemaManager,
      com.google.cloud.bigquery.Schema existingSchema,
      com.google.cloud.bigquery.Schema newSchema,
      com.google.cloud.bigquery.Schema expectedSchema) {
    testGetAndValidateProposedSchema(
        schemaManager, existingSchema, Collections.singletonList(newSchema), expectedSchema);
  }

  private void testGetAndValidateProposedSchema(
      SchemaManager schemaManager,
      com.google.cloud.bigquery.Schema existingSchema,
      List<com.google.cloud.bigquery.Schema> newSchemas,
      com.google.cloud.bigquery.Schema expectedSchema) {
    testGetAndValidateProposedSchema(schemaManager, existingSchema, newSchemas, expectedSchema,
        Collections.nCopies(newSchemas.size(), recordWithValueSchema(mockKafkaSchema)));
  }

  private void testGetAndValidateProposedSchema(
      SchemaManager schemaManager,
      com.google.cloud.bigquery.Schema existingSchema,
      List<com.google.cloud.bigquery.Schema> newSchemas,
      com.google.cloud.bigquery.Schema expectedSchema,
      List<SinkRecord> incomingSinkRecords) {
    Table existingTable = existingSchema != null ? tableWithSchema(existingSchema) : null;
    when(mockBigQuery.getTable(tableId)).thenReturn(existingTable);

    if (newSchemas != null) {
      OngoingStubbing<com.google.cloud.bigquery.Schema> converterStub =
          when(mockSchemaConverter.convertSchema(mockKafkaSchema));
      for (com.google.cloud.bigquery.Schema newSchema : newSchemas) {
        // The converter will return the schemas in the order that they are provided to it with the
        // call to "thenReturn"
        converterStub = converterStub.thenReturn(newSchema);
      }
    }

    com.google.cloud.bigquery.Schema proposedSchema = schemaManager
        .getAndValidateProposedSchema(tableId, incomingSinkRecords)
        .getSchema();

    if (expectedSchema != null) {
      assertEquals(expectedSchema, proposedSchema);
    }
  }

  private Table tableWithSchema(com.google.cloud.bigquery.Schema schema) {
    TableDefinition definition = mock(TableDefinition.class);
    when(definition.getSchema()).thenReturn(schema);

    Table result = mock(Table.class);
    when(result.getDefinition()).thenReturn(definition);

    return result;
  }

  private SinkRecord recordWithValueSchema(Schema valueSchema) {
    SinkRecord result = mock(SinkRecord.class);
    when(result.valueSchema()).thenReturn(valueSchema);
    return result;
  }

  @Test
  public void testUnionizeSchemaNoNestedOrRepeatedRecords() {
    com.google.cloud.bigquery.Schema s1 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.BYTES.name(), LegacySQLTypeName.BYTES),
        Field.of(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING),
        Field.of(LegacySQLTypeName.DATE.name(), LegacySQLTypeName.DATE)
    );
    com.google.cloud.bigquery.Schema s2 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.TIMESTAMP.name(), LegacySQLTypeName.TIMESTAMP),
        Field.of(LegacySQLTypeName.FLOAT.name(), LegacySQLTypeName.FLOAT)
    );

    List<Field> expectedFields = new ArrayList<>();
    expectedFields.addAll(s1.getFields());
    expectedFields.addAll(s2.getFields());

    assertUnion(makeNullable(com.google.cloud.bigquery.Schema.of(expectedFields)), s1, s2);
  }

  @Test
  public void testUnionizeSchemaWithNestedRecords() {
    com.google.cloud.bigquery.Schema s1 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.of(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING),
            Field.of(LegacySQLTypeName.DATE.name(), LegacySQLTypeName.DATE)
        ));
    com.google.cloud.bigquery.Schema s2 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.of(LegacySQLTypeName.TIMESTAMP.name(), LegacySQLTypeName.TIMESTAMP)
        ));
    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.of(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING),
            Field.of(LegacySQLTypeName.DATE.name(), LegacySQLTypeName.DATE),
            Field.of(LegacySQLTypeName.TIMESTAMP.name(), LegacySQLTypeName.TIMESTAMP)
        )
    );
    assertUnion(makeNullable(expected), s1, s2);
  }

  @Test
  public void testUnionizeSchemaWithNestedAndRepeatedFields() {
    com.google.cloud.bigquery.Schema s1 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build(),
            Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
                Field.of(LegacySQLTypeName.BYTES.name(), LegacySQLTypeName.BYTES)
            )
        )
    );
    com.google.cloud.bigquery.Schema s2 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.DATE.name(), LegacySQLTypeName.DATE).setMode(Mode.REPEATED).build(),
            Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
                Field.of(LegacySQLTypeName.FLOAT.name(), LegacySQLTypeName.FLOAT)
            )
        )
    );

    com.google.cloud.bigquery.Schema expected = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build(),
            Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
                Field.of(LegacySQLTypeName.BYTES.name(), LegacySQLTypeName.BYTES),
                Field.of(LegacySQLTypeName.FLOAT.name(), LegacySQLTypeName.FLOAT)
            ),
            Field.newBuilder(LegacySQLTypeName.DATE.name(), LegacySQLTypeName.DATE).setMode(Mode.REPEATED).build()
        )
    );
    assertUnion(makeNullable(expected), s1, s2);
  }

  @Test
  public void testUnionizeSchemaNestedRelax() {
    com.google.cloud.bigquery.Schema s1 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.REQUIRED).build()
        )
    );
    com.google.cloud.bigquery.Schema s2 = com.google.cloud.bigquery.Schema.of(
        Field.of(LegacySQLTypeName.RECORD.name(), LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build()
        )
    );
    assertUnion(makeNullable(s2), s1, s2);
  }

  @Test
  public void testUnionizeSchemaCaseInsensitive() {
    com.google.cloud.bigquery.Schema s1 = com.google.cloud.bigquery.Schema.of(
        Field.of("CAPS", LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.REQUIRED).build()
        )
    );
    com.google.cloud.bigquery.Schema s2 = com.google.cloud.bigquery.Schema.of(
        Field.of("caps", LegacySQLTypeName.RECORD,
            Field.newBuilder(LegacySQLTypeName.STRING.name(), LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build()
        )
    );
    assertUnion(makeNullable(s1), s1, s2);
  }

  @Test
  public void testFieldNameSanitizedOnCreateTable() {
    Schema embeddedStructWithInvalidFieldName = SchemaBuilder.struct()
        .field("embedded-invalid", Schema.INT32_SCHEMA)
        .build();
    Schema schemaWithInvalidFieldNames = SchemaBuilder.struct()
        .field("1st field", Schema.BOOLEAN_SCHEMA)
        .field("second-field", Schema.STRING_SCHEMA)
        .field("embedded", embeddedStructWithInvalidFieldName)
        .build();

    List<SinkRecord> incomingSinkRecords = Collections.nCopies(2, recordWithValueSchema(schemaWithInvalidFieldNames));
    BigQuerySchemaConverter converter = new BigQuerySchemaConverter(true, true);

    SchemaManager schemaManager = createSchemaManager(false, false, false, true, converter);
    schemaManager.createTable(tableId, incomingSinkRecords);

    ArgumentCaptor<TableInfo> tableInfoCaptor = ArgumentCaptor.forClass(TableInfo.class);

    verify(mockBigQuery).create(tableInfoCaptor.capture());

    com.google.cloud.bigquery.Schema actualSchema = tableInfoCaptor.getValue().getDefinition().getSchema();
    for (org.apache.kafka.connect.data.Field field : schemaWithInvalidFieldNames.fields()) {
      String sanitizedName = FieldNameSanitizer.sanitizeName(field.name());
      assertEquals(sanitizedName, actualSchema.getFields().get(sanitizedName).getName());
    }
    assertEquals("embedded_invalid", actualSchema.getFields().get("embedded").getSubFields().get(0).getName());
  }

  private com.google.cloud.bigquery.Schema makeNullable(com.google.cloud.bigquery.Schema s) {
    return com.google.cloud.bigquery.Schema.of(
        s.getFields().stream()
            .map(this::makeNullable)
            .collect(Collectors.toList())
    );
  }

  private Field makeNullable(Field f) {
    Field.Builder builder = f.toBuilder();
    if (f.getSubFields() != null) {
      List<Field> subFields = f.getSubFields().stream()
          .map(this::makeNullable)
          .collect(Collectors.toList());
      builder.setType(LegacySQLTypeName.RECORD, subFields.toArray(new Field[]{})).build();
    }
    return builder
        .setMode(f.getMode() == Mode.REPEATED ? Mode.REPEATED : Mode.NULLABLE)
        .build();
  }

  private void assertUnion(com.google.cloud.bigquery.Schema expected,
                           com.google.cloud.bigquery.Schema schema1, com.google.cloud.bigquery.Schema schema2) {
    SchemaManager sm = createSchemaManager(true, true, true);
    SchemaManager.SchemaAndPrimaryKeyColumns schemaAndPrimaryKeyColumns1 = new SchemaManager.SchemaAndPrimaryKeyColumns(
        schema1,
        Collections.emptyList()
    );
    SchemaManager.SchemaAndPrimaryKeyColumns schemaAndPrimaryKeyColumns2 = new SchemaManager.SchemaAndPrimaryKeyColumns(
        schema2,
        Collections.emptyList()
    );
    assertEquals(
        expected, sm.unionizeSchemas(schemaAndPrimaryKeyColumns1, schemaAndPrimaryKeyColumns2).getSchema()
    );
  }

}
