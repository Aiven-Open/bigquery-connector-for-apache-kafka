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

package com.wepay.kafka.connect.bigquery.convert.logicaltype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.MicroTimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.TimestampConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters.ZonedTimestampConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

public class DebeziumLogicalConvertersTest {

  //corresponds to March 1 2017, 22:20:38.808(123) UTC
  //              (March 1 2017, 14:20:38.808(123)-8:00)
  private static final Integer DAYS_TIMESTAMP = 17226;
  private static final Integer MILLI_TIMESTAMP_INT = 1488406838;
  private static final Long MILLI_TIMESTAMP = 1488406838808L;
  private static final Long MICRO_TIMESTAMP = 1488406838808123L;

  @Test
  public void testDateConversion() {
    DateConverter converter = new DateConverter();

    assertEquals(LegacySQLTypeName.DATE, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT32);

    String formattedDate = converter.convert(DAYS_TIMESTAMP);
    assertEquals("2017-03-01", formattedDate);
  }

  @Test
  public void testMicroTimeConversion() {
    testMicroTimeConversionHelper(MICRO_TIMESTAMP, "22:20:38.808123");
    // Test case where microseconds have a leading 0.
    long microTimestamp = 1592511382050720L;
    testMicroTimeConversionHelper(microTimestamp, "20:16:22.050720");
  }

  private void testMicroTimeConversionHelper(long microTimestamp, String s) {
    MicroTimeConverter converter = new MicroTimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT64);

    String formattedMicroTime = converter.convert(microTimestamp);
    assertEquals(s, formattedMicroTime);
  }


  @Test
  public void testMicroTimestampConversion() {
    testMicroTimestampConversionHelper(MICRO_TIMESTAMP, "2017-03-01 22:20:38.808123");
    // Test timestamp where microseconds have a leading 0
    Long timestamp = 1592511382050720L;
    testMicroTimestampConversionHelper(timestamp, "2020-06-18 20:16:22.050720");
  }

  private void testMicroTimestampConversionHelper(Long timestamp, String s) {
    MicroTimestampConverter converter = new MicroTimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT64);

    String formattedMicroTimestamp = converter.convert(timestamp);
    assertEquals(s, formattedMicroTimestamp);
  }

  @Test
  public void testTimeConversion() {
    TimeConverter converter = new TimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT32);

    String formattedTime = converter.convert(MILLI_TIMESTAMP_INT);
    assertEquals("05:26:46.838", formattedTime);
  }

  @Test
  public void testTimestampConversion() {
    TimestampConverter converter = new TimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT64);

    String formattedTimestamp = converter.convert(MILLI_TIMESTAMP);
    assertEquals("2017-03-01 22:20:38.808", formattedTimestamp);
  }

  @Test
  public void testZonedTimestampConversion() {
    ZonedTimestampConverter converter = new ZonedTimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.STRING);

    String formattedTimestamp = converter.convert("2017-03-01T14:20:38.808-08:00");
    assertEquals("2017-03-01 14:20:38.808-08:00", formattedTimestamp);
  }

  @Test
  public void testVariableScaleDecimalConversion() {
    DebeziumLogicalConverters.VariableScaleDecimalConverter converter =
        new DebeziumLogicalConverters.VariableScaleDecimalConverter();

    assertEquals(LegacySQLTypeName.NUMERIC, converter.getBqSchemaType());

    Schema schema = SchemaBuilder.struct()
        .name(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME)
        .field("scale", Schema.INT32_SCHEMA)
        .field("value", Schema.BYTES_SCHEMA)
        .build();

    converter.checkEncodingType(Schema.Type.STRUCT);

    Struct struct = new Struct(schema)
        .put("scale", 3)
        .put("value", new java.math.BigDecimal("123.456").unscaledValue().toByteArray());

    java.math.BigDecimal converted = (java.math.BigDecimal) converter.convert(struct);
    assertEquals(new java.math.BigDecimal("123.456"), converted);
  }

  @Test
  public void testVariableScaleDecimalConversionNullValue() {
    DebeziumLogicalConverters.VariableScaleDecimalConverter converter =
        new DebeziumLogicalConverters.VariableScaleDecimalConverter();

    Schema schema = SchemaBuilder.struct()
        .name(io.debezium.data.VariableScaleDecimal.LOGICAL_NAME)
        .field("scale", Schema.INT32_SCHEMA)
        .field("value", Schema.BYTES_SCHEMA)
        .build();

    converter.checkEncodingType(Schema.Type.STRUCT);

    java.math.BigDecimal converted = (java.math.BigDecimal) converter.convert(schema, null);
    assertNull(converted);
  }
}
