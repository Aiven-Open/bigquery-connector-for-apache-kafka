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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class BigQuerySchemaConverterTest {

  private boolean allFieldsNullable;
  private boolean sanitizeFieldNames;

  @BeforeEach
  void resetValues() {
    allFieldsNullable = false;
    sanitizeFieldNames = false;
    DebeziumLogicalConverters.remove();
    KafkaLogicalConverters.remove();
  }

  @AfterAll
  static void cleanUpConverters() {
    DebeziumLogicalConverters.remove();
    KafkaLogicalConverters.remove();
  }

  private static BigQuerySinkConfig testingConfig(boolean convertDebeziumTimestamp, BigQuerySinkConfig.DecimalHandlingMode varibaleScaleDecimalMode, BigQuerySinkConfig.DecimalHandlingMode decimalMode) {
    BigQuerySinkConfig result = mock(BigQuerySinkConfig.class);
    when(result.getVariableScaleDecimalHandlingMode()).thenReturn(varibaleScaleDecimalMode);
    when(result.getDecimalHandlingMode()).thenReturn(decimalMode);
    when(result.getShouldConvertDebeziumTimestampToInteger()).thenReturn(convertDebeziumTimestamp);
    return result;
  }

  private BigQuerySchemaConverter createConverter() {
    // use the defaults from sink config
    return createConverter(testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT));
  }

  private BigQuerySchemaConverter createConverter(BigQuerySinkConfig config) {
    DebeziumLogicalConverters.initialize(config);
    KafkaLogicalConverters.initialize(config);
    return new BigQuerySchemaConverter(allFieldsNullable, sanitizeFieldNames);
  }

  @Test
  public void testTopLevelSchema() {
    BigQuerySchemaConverter converter = createConverter();
    assertThrows(
        ConversionConnectException.class,
        () -> converter.convertSchema(Schema.BOOLEAN_SCHEMA)
    );
  }

  @Test
  public void testBoolean() {
    final String fieldName = "Boolean";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.BOOLEAN
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BOOLEAN_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testInteger() {
    final String fieldName = "Integer";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.INTEGER
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT8_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT16_SCHEMA)
        .build();

    bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT32_SCHEMA)
        .build();

    bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);


    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT64_SCHEMA)
        .build();

    bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testFloat() {
    final String fieldName = "Float";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.FLOAT
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT32_SCHEMA)
        .build();
    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);

    kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT64_SCHEMA)
        .build();

    bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testString() {
    final String fieldName = "String";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.STRING
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testStruct() { // Struct in a struct in a struct (wrapped in a struct)
    final String outerFieldStructName = "OuterStruct";
    final String middleFieldStructName = "MiddleStruct";
    final String middleFieldArrayName = "MiddleArray";
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";
    final String innerFieldIntegerName = "InnerInt";

    com.google.cloud.bigquery.Field bigQueryInnerRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            innerFieldStructName,
            LegacySQLTypeName.RECORD,
            com.google.cloud.bigquery.Field.newBuilder(
                innerFieldStringName,
                LegacySQLTypeName.STRING
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build(),
            com.google.cloud.bigquery.Field.newBuilder(
                innerFieldIntegerName,
                LegacySQLTypeName.INTEGER
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .field(innerFieldIntegerName, Schema.INT32_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedInnerSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryInnerRecord);
    com.google.cloud.bigquery.Schema bigQueryTestInnerSchema = createConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(innerFieldStructName, kafkaConnectInnerSchema)
                .build()
        );
    assertEquals(bigQueryExpectedInnerSchema, bigQueryTestInnerSchema);

    com.google.cloud.bigquery.Field bigQueryMiddleRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            middleFieldStructName,
            LegacySQLTypeName.RECORD,
            bigQueryInnerRecord,
            com.google.cloud.bigquery.Field.newBuilder(
                middleFieldArrayName,
                LegacySQLTypeName.FLOAT
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectMiddleSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldArrayName, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedMiddleSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryMiddleRecord);
    com.google.cloud.bigquery.Schema bigQueryTestMiddleSchema = createConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(middleFieldStructName, kafkaConnectMiddleSchema)
                .build()
        );
    assertEquals(bigQueryExpectedMiddleSchema, bigQueryTestMiddleSchema);

    com.google.cloud.bigquery.Field bigQueryOuterRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            outerFieldStructName,
            LegacySQLTypeName.RECORD,
            bigQueryInnerRecord,
            bigQueryMiddleRecord
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldStructName, kafkaConnectMiddleSchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedOuterSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryOuterRecord);
    com.google.cloud.bigquery.Schema bigQueryTestOuterSchema = createConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(outerFieldStructName, kafkaConnectOuterSchema)
                .build()
        );
    assertEquals(bigQueryExpectedOuterSchema, bigQueryTestOuterSchema);
  }

  @Test
  public void testEmptyStruct() { // Empty struct
    com.google.cloud.bigquery.Schema bigQueryTestOuterSchema = createConverter().convertSchema(
            SchemaBuilder
                .struct()
                .build()
        );
    assertEquals(com.google.cloud.bigquery.Schema.of(), bigQueryTestOuterSchema);
  }

  @Test
  public void testEmptyInnerStruct() { // Empty nested struct
    final String outerFieldStructName = "OuterStruct";
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .build();

    com.google.cloud.bigquery.Field bigQueryInnerString = com.google.cloud.bigquery.Field.newBuilder(
        innerFieldStringName,
        LegacySQLTypeName.STRING
    ).setMode(
        com.google.cloud.bigquery.Field.Mode.REQUIRED
    ).build();

    com.google.cloud.bigquery.Field bigQueryOuterRecord =
        com.google.cloud.bigquery.Field.newBuilder(
            outerFieldStructName,
            LegacySQLTypeName.RECORD,
            bigQueryInnerString
        ).setMode(
            com.google.cloud.bigquery.Field.Mode.REQUIRED
        ).build();

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedOuterSchema =
        com.google.cloud.bigquery.Schema.of(bigQueryOuterRecord);
    com.google.cloud.bigquery.Schema bigQueryTestOuterSchema = createConverter().convertSchema(
            SchemaBuilder
                .struct()
                .field(outerFieldStructName, kafkaConnectOuterSchema)
                .build()
        );
    assertEquals(bigQueryExpectedOuterSchema, bigQueryTestOuterSchema);
  }

  @Test
  public void testMap() {
    final String fieldName = "StringIntegerMap";
    final String keyName = BigQuerySchemaConverter.MAP_KEY_FIELD_NAME;
    final String valueName = BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME;

    Field floatField =
        Field.newBuilder(keyName, LegacySQLTypeName.FLOAT)
            .setMode(Field.Mode.REQUIRED)
            .build();
    Field stringField =
        Field.newBuilder(valueName, LegacySQLTypeName.STRING)
            .setMode(Field.Mode.REQUIRED)
            .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.RECORD,
                floatField,
                stringField
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REPEATED
            ).build()
        );


    Schema kafkaConnectMapSchema = SchemaBuilder
        .map(Schema.FLOAT32_SCHEMA, Schema.STRING_SCHEMA)
        .build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectMapSchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testIntegerArray() {
    final String fieldName = "IntegerArray";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.INTEGER
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        );

    Schema kafkaConnectArraySchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectArraySchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testStringArray() {
    final String fieldName = "StringArray";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.STRING
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        );

    Schema kafkaConnectArraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectArraySchema)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testFieldNameSanitized() {
    final String fieldName = "String Array";
    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                FieldNameSanitizer.sanitizeName(fieldName),
                LegacySQLTypeName.STRING
            ).setMode(com.google.cloud.bigquery.Field.Mode.REPEATED).build()
        );

    Schema kafkaConnectArraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, kafkaConnectArraySchema)
        .build();

    sanitizeFieldNames = true;
    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testBytes() {
    final String fieldName = "Bytes";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.BYTES
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BYTES_SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testTimestamp() {
    final String fieldName = "Timestamp";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.TIMESTAMP
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Timestamp.SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @ParameterizedTest
  @MethodSource({"testBadTimestampData"})
  public void testBadTimestamp(String name, String schemaName, BigQuerySinkConfig config) {
    final String fieldName = "Timestamp";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.bool().name(schemaName))
        .build();

    BigQuerySchemaConverter converter = createConverter(config);
    assertThrows(
        ConversionConnectException.class,
        () -> converter.convertSchema(kafkaConnectTestSchema)
    );
  }

  private static Stream<Arguments> testBadTimestampData() {
    List<Arguments> arguments = new ArrayList<>();

    arguments.add(Arguments.of(Timestamp.LOGICAL_NAME, Timestamp.LOGICAL_NAME, testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT)));
    arguments.add(Arguments.of(io.debezium.time.Timestamp.SCHEMA_NAME+" standard", io.debezium.time.Timestamp.SCHEMA_NAME, testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT)));
    arguments.add(Arguments.of(io.debezium.time.Timestamp.SCHEMA_NAME+" converted", io.debezium.time.Timestamp.SCHEMA_NAME, testingConfig(true, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT)));

    return arguments.stream();
  }

  @Test
  public void testDate() {
    final String fieldName = "Date";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.DATE
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Date.SCHEMA)
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testBadDate() {
    final String fieldName = "Date";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.int64().name(Date.LOGICAL_NAME))
        .build();

    BigQuerySchemaConverter converter = createConverter();
    assertThrows(
        ConversionConnectException.class,
        () -> converter.convertSchema(kafkaConnectTestSchema)
    );
  }

  @ParameterizedTest
  @EnumSource(BigQuerySinkConfig.DecimalHandlingMode.class)
  public void testDecimal(BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode) {
    final BigQuerySinkConfig config = testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, decimalHandlingMode);

    final String fieldName = "Decimal";

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, Decimal.schema(0))
        .build();

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema = null;
    switch (decimalHandlingMode) {
          case RECORD:
              FieldList fieldList = FieldList.of(
                      Field.newBuilder("scale", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build(),
                      Field.newBuilder("value", LegacySQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build());
              bigQueryExpectedSchema = com.google.cloud.bigquery.Schema.of(
                      com.google.cloud.bigquery.Field.newBuilder(fieldName,
                                      LegacySQLTypeName.RECORD,
                                      fieldList)
                              .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                              .build());
              break;
          case NUMERIC:
          case BIGNUMERIC:
              bigQueryExpectedSchema = com.google.cloud.bigquery.Schema.of(
                      com.google.cloud.bigquery.Field.newBuilder(
                              fieldName,
                              decimalHandlingMode.sqlTypeName
                      ).setMode(
                              com.google.cloud.bigquery.Field.Mode.REQUIRED
                      ).setScale(0L).build()
              );
              break;
          case FLOAT:
              bigQueryExpectedSchema = com.google.cloud.bigquery.Schema.of(
                      com.google.cloud.bigquery.Field.newBuilder(
                              fieldName,
                              decimalHandlingMode.sqlTypeName
                      ).setMode(
                              com.google.cloud.bigquery.Field.Mode.REQUIRED
                      ).build()
              );
              break;
          default:
              throw new ConversionConnectException("Unexpected DecimalHandlingMode: " + decimalHandlingMode);
    }
    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter(config).convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @ParameterizedTest
  @MethodSource("testBadDecimalData")
  public void testBadDecimal(String name, String schemaName, BigQuerySinkConfig config) {
    final String fieldName = "Decimal";

    Schema kafkaConnectTestSchema = SchemaBuilder
            .struct()
            .field(fieldName, SchemaBuilder.bool().name(schemaName))
            .build();

    BigQuerySchemaConverter converter = createConverter(config);
    assertThrows(
            ConversionConnectException.class,
            () -> converter.convertSchema(kafkaConnectTestSchema)
    );
  }

  private static Stream<Arguments> testBadDecimalData() {
    List<Arguments> arguments = new ArrayList<>();
    for (BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode : BigQuerySinkConfig.DecimalHandlingMode.values()) {
      BigQuerySinkConfig config = testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, decimalHandlingMode);
      arguments.add(Arguments.of(Decimal.LOGICAL_NAME + " " + config.getDecimalHandlingMode(), Decimal.LOGICAL_NAME, config));
    }
    for (BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode : BigQuerySinkConfig.DecimalHandlingMode.values()) {
      BigQuerySinkConfig config = testingConfig(false, decimalHandlingMode, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);
      arguments.add(Arguments.of(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME + " " + config.getVariableScaleDecimalHandlingMode(), io.debezium.data.VariableScaleDecimal.LOGICAL_NAME, config));
    }
    return arguments.stream();
  }


  @ParameterizedTest
  @EnumSource(BigQuerySinkConfig.DecimalHandlingMode.class)
  public void testDebeziumVariableScaleDecimal(BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode) {
      final String fieldName = "DebeziumDecimal";

      final BigQuerySinkConfig config = testingConfig(false, decimalHandlingMode, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);

      Schema variableDecimalSchema = SchemaBuilder.struct()
              .name(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME)
              .field("scale", Schema.INT32_SCHEMA)
              .field("value", Schema.BYTES_SCHEMA)
              .build();

      Schema kafkaConnectTestSchema = SchemaBuilder
              .struct()
              .field(fieldName, variableDecimalSchema)
              .build();

      com.google.cloud.bigquery.Schema bigQueryExpectedSchema = null;

      switch (decimalHandlingMode) {
          case RECORD:
              FieldList fieldList = FieldList.of(
                      Field.newBuilder("scale", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build(),
                      Field.newBuilder("value", LegacySQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build());
              bigQueryExpectedSchema = com.google.cloud.bigquery.Schema.of(
                      com.google.cloud.bigquery.Field.newBuilder(fieldName,
                                      LegacySQLTypeName.RECORD,
                                      fieldList)
                              .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                              .build());
              break;
          case NUMERIC:
          case BIGNUMERIC:
          case FLOAT:
              bigQueryExpectedSchema = com.google.cloud.bigquery.Schema.of(
                      com.google.cloud.bigquery.Field.newBuilder(
                              fieldName,
                              decimalHandlingMode.sqlTypeName
                      ).setMode(
                              com.google.cloud.bigquery.Field.Mode.REQUIRED
                      ).build()
              );
              break;
          default:
              throw new ConversionConnectException("Unexpected DecimalHandlingMode: " + decimalHandlingMode);
      }

      com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter(config).convertSchema(kafkaConnectTestSchema);
      assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testDebeziumTimestamp(boolean convertFlag) {
    final String fieldName = "DebeziumTimestamp";
    final BigQuerySinkConfig config = testingConfig(convertFlag, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
            com.google.cloud.bigquery.Schema.of(
                    com.google.cloud.bigquery.Field.newBuilder(
                            fieldName,
                            convertFlag ? LegacySQLTypeName.INTEGER : LegacySQLTypeName.TIMESTAMP
                    ).setMode(
                            com.google.cloud.bigquery.Field.Mode.REQUIRED
                    ).build()
            );

    Schema kafkaConnectTestSchema = SchemaBuilder
            .struct()
            .field(fieldName, io.debezium.time.Timestamp.schema())
            .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter(config).convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }


  @Test
  public void testNullable() {
    final String nullableFieldName = "Nullable";
    final String requiredFieldName = "Required";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                nullableFieldName,
                LegacySQLTypeName.INTEGER
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.NULLABLE
            ).build(),
            com.google.cloud.bigquery.Field.newBuilder(
                requiredFieldName,
                LegacySQLTypeName.INTEGER
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.REQUIRED
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(nullableFieldName, SchemaBuilder.int32().optional().build())
        .field(requiredFieldName, SchemaBuilder.int32().required().build())
        .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testDescription() {
    final String fieldName = "WithDoc";
    final String fieldDoc = "test documentation";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(fieldName,
                    LegacySQLTypeName.STRING)
                .setMode(com.google.cloud.bigquery.Field.Mode.REQUIRED)
                .setDescription(fieldDoc)
                .build()
        );

    Schema kafkaConnectTestSchema =
        SchemaBuilder.struct()
            .field(fieldName, SchemaBuilder.string().doc(fieldDoc).build())
            .build();

    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testAllFieldsNullable() {
    final String fieldName = "RequiredField";

    com.google.cloud.bigquery.Schema bigQueryExpectedSchema =
        com.google.cloud.bigquery.Schema.of(
            com.google.cloud.bigquery.Field.newBuilder(
                fieldName,
                LegacySQLTypeName.STRING
            ).setMode(
                com.google.cloud.bigquery.Field.Mode.NULLABLE
            ).build()
        );

    Schema kafkaConnectTestSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.string().required().build())
        .build();

    allFieldsNullable = true;
    com.google.cloud.bigquery.Schema bigQueryTestSchema = createConverter().convertSchema(kafkaConnectTestSchema);
    assertEquals(bigQueryExpectedSchema, bigQueryTestSchema);
  }

  @Test
  public void testSimpleRecursiveSchemaThrows() {
    final String fieldName = "RecursiveField";

    // Construct Avro schema with recursion since we cannot directly construct Connect schema with cycle
    org.apache.avro.Schema recursiveAvroSchema = org.apache.avro.SchemaBuilder
        .record("RecursiveItem")
        .namespace("com.example")
        .fields()
        .name(fieldName)
        .type().unionOf().nullType().and().type("RecursiveItem").endUnion()
        .nullDefault()
        .endRecord();

    Schema connectSchema = new AvroData(100).toConnectSchema(recursiveAvroSchema);
    ConversionConnectException e = assertThrows(ConversionConnectException.class, () -> createConverter().convertSchema(connectSchema));
    assertEquals("Kafka Connect schema contains cycle", e.getMessage());
  }

  @Test
  public void testComplexRecursiveSchemaThrows() {
    final String fieldName = "RecursiveField";

    // Construct Avro schema with recursion since we cannot directly construct Connect schema with cycle
    org.apache.avro.Schema recursiveAvroSchema = org.apache.avro.SchemaBuilder
        .record("RecursiveItem")
        .namespace("com.example")
        .fields()
        .name(fieldName)
        .type()
        .array().items()
        .map().values().type("RecursiveItem").noDefault()
        .endRecord();

    Schema connectSchema = new AvroData(100).toConnectSchema(recursiveAvroSchema);
    ConversionConnectException e = assertThrows(ConversionConnectException.class, () ->createConverter().convertSchema(connectSchema));
    assertEquals("Kafka Connect schema contains cycle", e.getMessage());
  }
}
