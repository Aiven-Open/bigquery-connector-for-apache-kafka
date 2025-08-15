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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DecimalConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimestampConverter;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class KafkaLogicalConvertersTest {

  //corresponds to March 1 2017, 22:20:38.808
  private static final Long TIMESTAMP = 1488406838808L;

  @Test
  public void testDateConversion() {
    DateConverter converter = new DateConverter();

    assertEquals(LegacySQLTypeName.DATE, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT32);

    Date date = new Date(TIMESTAMP);
    String formattedDate = converter.convert(date);
    assertEquals("2017-03-01", formattedDate);
  }

  @ParameterizedTest
  @EnumSource(BigQuerySinkConfig.DecimalHandlingMode.class)
  public void testDecimalConversion(BigQuerySinkConfig.DecimalHandlingMode handlingMode) {
    DecimalConverter converter = new DecimalConverter(handlingMode);

    assertEquals(handlingMode.sqlTypeName, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.BYTES);

    final int scale = 5;
    final Schema connectDecimalSchema = Decimal.builder(scale).build();
    final BigDecimal bigDecimal = new BigDecimal("3.14159");

    byte[] bytes = Decimal.fromLogical(connectDecimalSchema, bigDecimal);
    BigDecimal kafkaConnectObject = Decimal.toLogical(connectDecimalSchema, bytes);

    Object convertedDecimal = converter.convert(kafkaConnectObject);

    switch (handlingMode) {
      case RECORD:
        assertInstanceOf(Map.class, convertedDecimal);
        Map<?, ?> struct = (Map<?, ?>) convertedDecimal;
        assertEquals(bigDecimal.scale(), struct.get("scale"));
        assertArrayEquals(bigDecimal.unscaledValue().toByteArray(), (byte[]) struct.get("value"));
        break;
      case BIGNUMERIC:
      case NUMERIC:
        assertInstanceOf(BigDecimal.class, convertedDecimal);
        assertEquals(bigDecimal, convertedDecimal);
        break;
      case FLOAT:
        assertInstanceOf(Double.class, convertedDecimal);
        assertEquals(3.14159, convertedDecimal);
        break;
      default:
        throw new UnsupportedOperationException(handlingMode.name());
    }
  }

  @Test
  public void testTimestampConversion() {
    TimestampConverter converter = new TimestampConverter();

    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT64);

    assertThrows(
        Exception.class,
        () -> converter.checkEncodingType(Schema.Type.INT32)
    );

    Date date = new Date(TIMESTAMP);
    String formattedTimestamp = converter.convert(date);

    assertEquals("2017-03-01 22:20:38.808", formattedTimestamp);
  }


  @Test
  public void testTimeConversion() {
    TimeConverter converter = new KafkaLogicalConverters.TimeConverter();

    assertEquals(LegacySQLTypeName.TIME, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.INT32);

    assertThrows(
        Exception.class,
        () -> converter.checkEncodingType(Schema.Type.INT64)
    );

    // Can't use the same timestamp here as the one in other tests as the Time type
    // should only fall on January 1st, 1970
    Date date = new Date(166838808);
    String formattedTimestamp = converter.convert(date);

    assertEquals("22:20:38.808", formattedTimestamp);
  }
}
