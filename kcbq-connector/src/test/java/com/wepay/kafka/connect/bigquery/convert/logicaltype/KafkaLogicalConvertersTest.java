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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DateConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.DecimalConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimeConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters.TimestampConverter;
import java.math.BigDecimal;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Decimal;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testDecimalConversion() {
    DecimalConverter converter = new DecimalConverter();

    assertEquals(LegacySQLTypeName.FLOAT, converter.getBqSchemaType());

    converter.checkEncodingType(Schema.Type.BYTES);

    BigDecimal bigDecimal = new BigDecimal("3.14159");

    BigDecimal convertedDecimal = converter.convert(bigDecimal);

    // expecting no-op
    assertEquals(bigDecimal, convertedDecimal);

    Schema schema = Decimal.builder(2).parameter("connect.decimal.precision", "10").build();
    Field field = converter.getFieldBuilder(schema, "foo").build();
    assertEquals(LegacySQLTypeName.NUMERIC, field.getType());
    assertEquals(Long.valueOf(2L), field.getScale());
    assertEquals(Long.valueOf(10L), field.getPrecision());

 
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
