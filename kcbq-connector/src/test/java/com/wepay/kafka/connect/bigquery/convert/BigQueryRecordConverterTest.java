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

import com.google.protobuf.ByteString;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BigQueryRecordConverterTest {

  private boolean shouldConvertDouble = true;
  private boolean useStorageWriteApiConfig = false;

  @BeforeEach
  void resetValues() {
    shouldConvertDouble = true;
    useStorageWriteApiConfig = false;
  }

  private static SinkRecord spoofSinkRecord(Schema schema, Object struct, boolean isKey) {
    if (isKey) {
      return new SinkRecord(null, 0, schema, struct, null, null, 0);
    }
    return new SinkRecord(null, 0, null, null, schema, struct, 0);
  }

  private BigQueryRecordConverter createConverter() {
    // use the defaults from sink config
    return createConverter(testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.RECORD, BigQuerySinkConfig.DecimalHandlingMode.FLOAT));
  }

  private BigQueryRecordConverter createConverter(BigQuerySinkConfig config) {
    DebeziumLogicalConverters.initialize(config);
    KafkaLogicalConverters.initialize(config);
    return new BigQueryRecordConverter(shouldConvertDouble, useStorageWriteApiConfig);
  }

  @Test
  public void testTopLevelRecord() {
    SinkRecord kafkaConnectRecord = spoofSinkRecord(Schema.BOOLEAN_SCHEMA, false, false);
    BigQueryRecordConverter converter = createConverter();
    assertThrows(
        ConversionConnectException.class,
        () -> converter.convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE)
    );
  }

  @Test
  public void testBoolean() {
    final String fieldName = "Boolean";
    final Boolean fieldValue = true;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BOOLEAN_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testInteger() {
    final String fieldName = "Integer";
    final Byte fieldByteValue = (byte) 42;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldByteValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT8_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldByteValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Short fieldShortValue = (short) 4242;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldShortValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT16_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldShortValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Integer fieldIntegerValue = 424242;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldIntegerValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT32_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldIntegerValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Long fieldLongValue = 424242424242L;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldLongValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT64_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldLongValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testFloat() {
    final String fieldName = "Float";
    final Float fieldFloatValue = 4242424242.4242F;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldFloatValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT32_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldFloatValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Double fieldDoubleValue = 4242424242.4242;

    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldDoubleValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT64_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldDoubleValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testDoubleSpecial() {
    final String fieldName = "Double";

    List<Double> testValues =
        Arrays.asList(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN);
    List<Double> expectedValues =
        Arrays.asList(Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_VALUE);
    assertEquals(testValues.size(), expectedValues.size());

    for (int test = 0; test < testValues.size(); ++test) {
      Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
      bigQueryExpectedRecord.put(fieldName, expectedValues.get(test));

      Schema kafkaConnectSchema = SchemaBuilder
          .struct()
          .field(fieldName, Schema.FLOAT64_SCHEMA)
          .build();

      Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
      kafkaConnectStruct.put(fieldName, testValues.get(test));
      SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

      Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
      assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
    }
  }

  @Test
  public void testString() {
    final String fieldName = "String";
    final String fieldValue = "42424242424242424242424242424242";

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testStruct() {
    final String middleFieldStructName = "MiddleStruct";
    final String middleFieldArrayName = "MiddleArray";
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";
    final String innerFieldIntegerName = "InnerInt";
    final String innerStringValue = "forty two";
    final Integer innerIntegerValue = 42;
    final List<Float> middleArrayValue = Arrays.asList(42.0f, 42.4f, 42.42f, 42.424f, 42.4242f);

    Map<String, Object> bigQueryExpectedInnerRecord = new HashMap<>();
    bigQueryExpectedInnerRecord.put(innerFieldStringName, innerStringValue);
    bigQueryExpectedInnerRecord.put(innerFieldIntegerName, innerIntegerValue);

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .field(innerFieldIntegerName, Schema.INT32_SCHEMA)
        .build();

    Struct kafkaConnectInnerStruct = new Struct(kafkaConnectInnerSchema);
    kafkaConnectInnerStruct.put(innerFieldStringName, innerStringValue);
    kafkaConnectInnerStruct.put(innerFieldIntegerName, innerIntegerValue);

    SinkRecord kafkaConnectInnerSinkRecord =
        spoofSinkRecord(kafkaConnectInnerSchema, kafkaConnectInnerStruct, false);
    Map<String, Object> bigQueryTestInnerRecord = createConverter().convertRecord(kafkaConnectInnerSinkRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedInnerRecord, bigQueryTestInnerRecord);


    Map<String, Object> bigQueryExpectedMiddleRecord = new HashMap<>();
    bigQueryExpectedMiddleRecord.put(innerFieldStructName, bigQueryTestInnerRecord);
    bigQueryExpectedMiddleRecord.put(middleFieldArrayName, middleArrayValue);

    Schema kafkaConnectMiddleSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldArrayName, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
        .build();

    Struct kafkaConnectMiddleStruct = new Struct(kafkaConnectMiddleSchema);
    kafkaConnectMiddleStruct.put(innerFieldStructName, kafkaConnectInnerStruct);
    kafkaConnectMiddleStruct.put(middleFieldArrayName, middleArrayValue);

    SinkRecord kafkaConnectMiddleSinkRecord =
        spoofSinkRecord(kafkaConnectMiddleSchema, kafkaConnectMiddleStruct, true);
    Map<String, Object> bigQueryTestMiddleRecord = createConverter().convertRecord(kafkaConnectMiddleSinkRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedMiddleRecord, bigQueryTestMiddleRecord);


    Map<String, Object> bigQueryExpectedOuterRecord = new HashMap<>();
    bigQueryExpectedOuterRecord.put(innerFieldStructName, bigQueryTestInnerRecord);
    bigQueryExpectedOuterRecord.put(middleFieldStructName, bigQueryTestMiddleRecord);

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldStructName, kafkaConnectMiddleSchema)
        .build();

    Struct kafkaConnectOuterStruct = new Struct(kafkaConnectOuterSchema);
    kafkaConnectOuterStruct.put(innerFieldStructName, kafkaConnectInnerStruct);
    kafkaConnectOuterStruct.put(middleFieldStructName, kafkaConnectMiddleStruct);

    SinkRecord kafkaConnectOuterSinkRecord =
        spoofSinkRecord(kafkaConnectOuterSchema, kafkaConnectOuterStruct, false);
    Map<String, Object> bigQueryTestOuterRecord = createConverter().convertRecord(kafkaConnectOuterSinkRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedOuterRecord, bigQueryTestOuterRecord);
  }

  @Test
  public void testEmptyStruct() {
    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .build();

    Struct kafkaConnectInnerStruct = new Struct(kafkaConnectInnerSchema);

    SinkRecord kafkaConnectSinkRecord =
        spoofSinkRecord(kafkaConnectInnerSchema, kafkaConnectInnerStruct, false);
    Map<String, Object> bigQueryTestInnerRecord = createConverter().convertRecord(kafkaConnectSinkRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(new HashMap<String, Object>(), bigQueryTestInnerRecord);
  }

  @Test
  public void testEmptyInnerStruct() {
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";
    final String innerStringValue = "forty two";

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .build();

    Struct kafkaConnectInnerStruct = new Struct(kafkaConnectInnerSchema);

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .build();

    Struct kafkaConnectOuterStruct = new Struct(kafkaConnectOuterSchema);
    kafkaConnectOuterStruct.put(innerFieldStructName, kafkaConnectInnerStruct);
    kafkaConnectOuterStruct.put(innerFieldStringName, innerStringValue);

    Map<String, Object> bigQueryExpectedOuterRecord = new HashMap<>();
    bigQueryExpectedOuterRecord.put(innerFieldStringName, innerStringValue);

    SinkRecord kafkaConnectOuterSinkRecord =
        spoofSinkRecord(kafkaConnectOuterSchema, kafkaConnectOuterStruct, false);
    Map<String, Object> bigQueryTestOuterRecord = createConverter().convertRecord(kafkaConnectOuterSinkRecord, KafkaSchemaRecordType.VALUE);

    assertEquals(bigQueryExpectedOuterRecord, bigQueryTestOuterRecord);
  }

  @Test
  public void testMap() {
    final String fieldName = "StringIntegerMap";
    final Map<Integer, Boolean> fieldValueKafkaConnect = new HashMap<>();
    final List<Map<String, Object>> fieldValueBigQuery = new ArrayList<>();

    for (int n = 2; n <= 10; n++) {
      boolean isPrime = true;
      for (int d : fieldValueKafkaConnect.keySet()) {
        if (n % d == 0) {
          isPrime = false;
          break;
        }
      }
      fieldValueKafkaConnect.put(n, isPrime);
      Map<String, Object> entryBigQuery = new HashMap<>();
      entryBigQuery.put(BigQuerySchemaConverter.MAP_KEY_FIELD_NAME, n);
      entryBigQuery.put(BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME, isPrime);
      fieldValueBigQuery.add(entryBigQuery);
    }

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.BOOLEAN_SCHEMA))
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testIntegerArray() {
    final String fieldName = "IntegerArray";
    final List<Integer> fieldValue = Arrays.asList(42, 4242, 424242, 42424242);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testStructArray() {
    final String innerFieldStringName = "InnerString";
    final String innerFieldIntegerName = "InnerInt";
    final String innerStringValue = "42";
    final Integer innerIntegerValue = 42;
    Map<String, Object> bigQueryExpectedInnerRecord = new HashMap<>();
    bigQueryExpectedInnerRecord.put(innerFieldStringName, innerStringValue);
    bigQueryExpectedInnerRecord.put(innerFieldIntegerName, innerIntegerValue);

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .field(innerFieldIntegerName, Schema.INT32_SCHEMA)
        .build();

    Struct kafkaConnectInnerStruct = new Struct(kafkaConnectInnerSchema);
    kafkaConnectInnerStruct.put(innerFieldStringName, innerStringValue);
    kafkaConnectInnerStruct.put(innerFieldIntegerName, innerIntegerValue);

    SinkRecord kafkaConnectInnerSinkRecord =
        spoofSinkRecord(kafkaConnectInnerSchema, kafkaConnectInnerStruct, true);
    Map<String, Object> bigQueryTestInnerRecord = createConverter().convertRecord(kafkaConnectInnerSinkRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedInnerRecord, bigQueryTestInnerRecord);

    final String middleFieldArrayName = "MiddleArray";
    final List<Map<String, Object>> fieldValue = Collections.singletonList(bigQueryTestInnerRecord);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(middleFieldArrayName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(middleFieldArrayName, SchemaBuilder.array(kafkaConnectInnerSchema).build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(middleFieldArrayName, Collections.singletonList(kafkaConnectInnerStruct));
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testStringArray() {
    final String fieldName = "StringArray";
    final List<String> fieldValue =
        Arrays.asList("Forty-two", "forty-two", "Forty two", "forty two");

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testBytes() {
    final String fieldName = "Bytes";
    final byte[] fieldBytes = new byte[]{42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54};
    final ByteBuffer fieldValueKafkaConnect = ByteBuffer.wrap(fieldBytes);
    final String fieldValueBigQuery = Base64.getEncoder().encodeToString(fieldBytes);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BYTES_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testByteString() {
    final String fieldName = "Bytes";
    final byte[] fieldBytes = new byte[]{42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54};
    final ByteBuffer fieldValueKafkaConnect = ByteBuffer.wrap(fieldBytes);
    final ByteString fieldValueBigQuery = ByteString.copyFrom(fieldBytes);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BYTES_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);
    useStorageWriteApiConfig = true;
    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @ParameterizedTest
  @MethodSource("testDebeziumLogicalTypeData")
  void testDebeziumLogicalType(String testName, BigQuerySinkConfig config, SinkRecord kafkaConnectRecord, Map<String, Object> bigQueryExpectedRecord) {
    DebeziumLogicalConverters.initialize(config);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter(shouldConvertDouble, useStorageWriteApiConfig).convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  private static SinkRecord createSinkRecord(String fieldName, Object fieldValue, Schema schema) {
    Schema kafkaConnectSchema = SchemaBuilder
            .struct()
            .field(fieldName, schema)
            .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    return spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);
  }

  private static Stream<Arguments> testDebeziumLogicalTypeData() {
    List<Arguments> arguments = new ArrayList<>();

    BigQuerySinkConfig config = testingConfig(false, BigQuerySinkConfig.DecimalHandlingMode.NUMERIC, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);

    String fieldName = "DebeziumDate";

    Map<String, Object> expectedRecord = new HashMap<>();
    expectedRecord.put(fieldName, "2017-03-01");

    SinkRecord kafkaConnectRecord = createSinkRecord(fieldName, 17226, io.debezium.time.Date.schema());
    arguments.add(Arguments.of("int to Debezium date", config, kafkaConnectRecord, expectedRecord));

    // Unconverted timestamp
    fieldName = "DebeziumTimestamp";

    expectedRecord = new HashMap<>();
    expectedRecord.put(fieldName, "2021-01-28 17:29:04.000");

    kafkaConnectRecord = createSinkRecord(fieldName, 1611854944000L, io.debezium.time.Timestamp.schema());
    arguments.add(Arguments.of("Unconverted timestamp", config, kafkaConnectRecord, expectedRecord));

    // Converted timestamp
    // By default, it is set to false
    config = testingConfig(true, BigQuerySinkConfig.DecimalHandlingMode.NUMERIC, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);
    expectedRecord = new HashMap<>();
    expectedRecord.put(fieldName, 1611854944000L);
    arguments.add(Arguments.of("Converted timestamp", config, kafkaConnectRecord, expectedRecord));

    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource("testDebeziumVariableScaleData")
  public void testDebeziumVariableScaleDecimal(String name, BigQuerySinkConfig config, Object expectedValue) {
    DebeziumLogicalConverters.initialize(config);

    final String fieldName = "DebeziumDecimal";
    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
      bigQueryExpectedRecord.put(fieldName, expectedValue);

      Schema variableDecimalSchema = SchemaBuilder.struct()
        .name(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME)
        .field("scale", Schema.INT32_SCHEMA)
        .field("value", Schema.BYTES_SCHEMA)
        .build();

    Struct decimalStruct = new Struct(variableDecimalSchema)
        .put("scale", 2)
        .put("value", new java.math.BigDecimal("12.34").unscaledValue().toByteArray());

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, variableDecimalSchema)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, decimalStruct);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter(config).convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  private static Stream<Arguments> testDebeziumVariableScaleData() {
    List<Arguments> arguments = new ArrayList<>();
    for (BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode : BigQuerySinkConfig.DecimalHandlingMode.values()) {
      Object expectedValue;
      switch (decimalHandlingMode) {
        case NUMERIC:
        case BIGNUMERIC:
          expectedValue = new java.math.BigDecimal("12.34");
          break;
          case FLOAT:
            expectedValue = 12.34;
            break;
        case RECORD:
          Schema variableDecimalSchema = SchemaBuilder.struct()
                  .name(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME)
                  .field("scale", Schema.INT32_SCHEMA)
                  .field("value", Schema.BYTES_SCHEMA)
                  .build();

          expectedValue = new Struct(variableDecimalSchema)
                  .put("scale", 2)
                  .put("value", new java.math.BigDecimal("12.34").unscaledValue().toByteArray());
          break;
        default:
          throw new UnsupportedOperationException("Unsupported decimal handling mode: " + decimalHandlingMode);
      }
      BigQuerySinkConfig config =  testingConfig(false, decimalHandlingMode, BigQuerySinkConfig.DecimalHandlingMode.FLOAT);
      arguments.add(Arguments.of(decimalHandlingMode.name(), config, expectedValue));
    }
    return arguments.stream();
  }

  @Test
  public void testKafkaLogicalType() {
    final String fieldName = "KafkaDate";
    final Date fieldDate = new Date(1488406838808L);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, "2017-03-01");

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, org.apache.kafka.connect.data.Date.SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldDate);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

  }

  @Test
  public void testNullable() {
    final String nullableFieldName = "nullable";
    final String requiredFieldName = "required";
    final Integer nullableFieldValue = null;
    final Integer requiredFieldValue = 42;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(requiredFieldName, requiredFieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(nullableFieldName, SchemaBuilder.int32().optional().build())
        .field(requiredFieldName, SchemaBuilder.int32().required().build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(nullableFieldName, nullableFieldValue);
    kafkaConnectStruct.put(requiredFieldName, requiredFieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, true);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testNullableStruct() {
    final String nullableFieldName = "nullableStruct";

    final Map<String, Object> bigQueryExpectedRecord = new HashMap<>();

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(nullableFieldName,
            SchemaBuilder.struct().field("foobar",
                SchemaBuilder.bool().build()).optional().build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(nullableFieldName, null);

    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct, false);

    Map<String, Object> bigQueryTestRecord = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testValidMapSchemaless() {
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", "f2");
      put("f3",
          new HashMap<Object, Object>() {{
            put("f4", "false");
            put("f5", true);
            put("f6", new ArrayList<String>() {{
              add("hello");
              add("world");
            }});
          }}
      );
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, true);
    Map<String, Object> convertedMap = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(kafkaConnectMap, convertedMap);
  }

  @Test
  public void testInvalidMapSchemaless() {
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", "f2");
      put("f3",
          new HashMap<Object, Object>() {{
            put(1, "false");
            put("f5", true);
            put("f6", new ArrayList<String>() {{
              add("hello");
              add("world");
            }});
          }}
      );
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, false);
    BigQueryRecordConverter converter =  createConverter();
    assertThrows(
        ConversionConnectException.class,
        () -> converter.convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE)
    );
  }

  @Test
  public void testInvalidMapSchemalessNullValue() {
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", "abc");
      put("f2", "abc");
      put("f3", null);
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, true);
    BigQueryRecordConverter converter =  createConverter();
    Map<String, Object> stringObjectMap = converter.convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(kafkaConnectMap, stringObjectMap);
  }

  @Test
  public void testInvalidMapSchemalessNestedMapNullValue() {
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", "abc");
      put("f2", "abc");
      put("f3", new HashMap<Object, Object>() {{
        put("f31", "xyz");
        put("f32", null);
      }});
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, true);
    BigQueryRecordConverter converter =  createConverter();
    Map<String, Object> stringObjectMap = converter.convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(kafkaConnectMap, stringObjectMap);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapSchemalessConvertDouble() {
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", Double.POSITIVE_INFINITY);
      put("f3",
          new HashMap<Object, Object>() {{
            put("f4", Double.POSITIVE_INFINITY);
            put("f5", true);
            put("f6", new ArrayList<Double>() {{
              add(1.2);
              add(Double.POSITIVE_INFINITY);
            }});
          }}
      );
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, true);
    Map<String, Object> convertedMap = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.KEY);
    assertEquals(Double.MAX_VALUE, convertedMap.get("f1"));
    assertEquals(Double.MAX_VALUE, ((Map<Object, Object>) (convertedMap.get("f3"))).get("f4"));
    assertEquals(Double.MAX_VALUE, ((ArrayList<Object>) ((Map<Object, Object>) (convertedMap.get("f3"))).get("f6")).get(1));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapSchemalessConvertBytes() {
    byte[] helloWorld = "helloWorld".getBytes();
    ByteBuffer helloWorldBuffer = ByteBuffer.wrap(helloWorld);
    Map<Object, Object> kafkaConnectMap = new HashMap<Object, Object>() {{
      put("f1", helloWorldBuffer);
      put("f3",
          new HashMap<Object, Object>() {{
            put("f4", helloWorld);
            put("f5", true);
            put("f6", new ArrayList<Double>() {{
              add(1.2);
              add(Double.POSITIVE_INFINITY);
            }});
          }}
      );
    }};

    SinkRecord kafkaConnectRecord = spoofSinkRecord(null, kafkaConnectMap, false);
    Map<String, Object> convertedMap = createConverter().convertRecord(kafkaConnectRecord, KafkaSchemaRecordType.VALUE);
    assertEquals(convertedMap.get("f1"), Base64.getEncoder().encodeToString(helloWorld));
    assertEquals(((Map<Object, Object>) (convertedMap.get("f3"))).get("f4"), Base64.getEncoder().encodeToString(helloWorld));
  }

  private static BigQuerySinkConfig testingConfig(boolean convertDebeziumTimestamp, BigQuerySinkConfig.DecimalHandlingMode varibaleScaleDecimalMode, BigQuerySinkConfig.DecimalHandlingMode decimalMode) {
    BigQuerySinkConfig result = mock(BigQuerySinkConfig.class);
    when(result.getVariableScaleDecimalHandlingMode()).thenReturn(varibaleScaleDecimalMode);
    when(result.getDecimalHandlingMode()).thenReturn(decimalMode);
    when(result.getShouldConvertDebeziumTimestampToInteger()).thenReturn(convertDebeziumTimestamp);
    return result;
  }
}
