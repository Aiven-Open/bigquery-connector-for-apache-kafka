/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
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

import com.google.common.collect.ImmutableList;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import java.util.Random;
import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

  @Before
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
        mockBigQuery, false, false, false, kafkaKeyFieldName, kafkaDataFieldName, Optional.empty(), Optional.empty());

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc);

    assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    Assert.assertNull("Timestamp partition field name is not null",
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField());
  }

  @Test
  public void testTimestampPartitionSet() {
    Optional<String> testField = Optional.of("testField");
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), testField, Optional.empty());

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc);

    assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        ((StandardTableDefinition) tableInfo.getDefinition()).getTimePartitioning().getField());
  }

  @Test
  public void testClusteringPartitionSet() {
    Optional<String> timestampPartitionFieldName = Optional.of("testField");
    Optional<List<String>> testField = Optional.of(Arrays.asList("column1", "column2"));
    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever, mockSchemaConverter,
        mockBigQuery, false, false, false, Optional.empty(), Optional.empty(), timestampPartitionFieldName, testField);

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager
        .constructTableInfo(tableId, fakeBigQuerySchema, testDoc);

    assertEquals("Kafka doc does not match BigQuery table description",
        testDoc, tableInfo.getDescription());
    StandardTableDefinition definition = tableInfo.getDefinition();
    Assert.assertNotNull(definition.getClustering());
    assertEquals("The field name does not match the field name of time partition",
        testField.get(),
        definition.getClustering().getFields());
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

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyRelaxedFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema relaxedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, relaxedSchema, null);
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

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyNewFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUpdateWithOnlyNewRequiredFields() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, false);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
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

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUnionizedUpdateWithNewField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build(),
        Field.newBuilder("f2", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(false, true, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
  }

  @Test(expected = BigQueryConnectException.class)
  public void testDisallowedUnionizedUpdateWithRelaxedField() {
    com.google.cloud.bigquery.Schema existingSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.REQUIRED).build()
    );

    com.google.cloud.bigquery.Schema expandedSchema = com.google.cloud.bigquery.Schema.of(
        Field.newBuilder("f1", LegacySQLTypeName.BOOLEAN).setMode(Field.Mode.NULLABLE).build()
    );

    SchemaManager schemaManager = createSchemaManager(true, false, true);

    testGetAndValidateProposedSchema(schemaManager, existingSchema, expandedSchema, null);
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

  @Test(expected = BigQueryConnectException.class)
  public void testUpdateWithOnlyTombstoneRecordsNoExistingSchema() {
    SchemaManager schemaManager = createSchemaManager(true, false, false);
    List<SinkRecord> incomingSinkRecords = Collections.nCopies(2, recordWithValueSchema(null));
    testGetAndValidateProposedSchema(
        schemaManager, null, Collections.singletonList(null), null, incomingSinkRecords);
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
    Assert.assertNull(schemaManager.getUnionizedTableDescription(incomingSinkRecords));
  }

  @Test
  public void testGetUnionizedTableDescriptionFromRegularAndNullRecords() {
    SchemaManager schemaManager = createSchemaManager(false, true, true).forIntermediateTables();
    List<SinkRecord> incomingSinkRecords = ImmutableList.of(
        recordWithValueSchema(mockKafkaSchema), recordWithValueSchema(null));
    when(mockKafkaSchema.doc()).thenReturn(testDoc);
    Assert.assertNotNull(schemaManager.getUnionizedTableDescription(incomingSinkRecords));
  }

  private SchemaManager createSchemaManager(
      boolean allowNewFields, boolean allowFieldRelaxation, boolean allowUnionization) {
    return new SchemaManager(new IdentitySchemaRetriever(), mockSchemaConverter, mockBigQuery,
        allowNewFields, allowFieldRelaxation, allowUnionization,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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
    OngoingStubbing<com.google.cloud.bigquery.Schema> converterStub =
        when(mockSchemaConverter.convertSchema(mockKafkaSchema));
    for (com.google.cloud.bigquery.Schema newSchema : newSchemas) {
      // The converter will return the schemas in the order that they are provided to it with the
      // call to "thenReturn"
      converterStub = converterStub.thenReturn(newSchema);
    }

    com.google.cloud.bigquery.Schema proposedSchema =
        schemaManager.getAndValidateProposedSchema(tableId, incomingSinkRecords);

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
    assertEquals(
        expected, sm.unionizeSchemas(schema1, schema2)
    );
  }

}
