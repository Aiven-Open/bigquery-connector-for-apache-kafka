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

import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.LocalTimestampMicrosConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.LocalTimestampMillisConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.LocalTimestampNanosConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.TimeMicrosConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.TimestampMicrosConverter;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters.TimestampNanosConverter;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

public class AvroLogicalConvertersTest {

  // 2017-03-01 22:20:38.808123 UTC
  private static final long TIMESTAMP_MICROS = 1488406838_808_123L;
  // same moment in nanos
  private static final long TIMESTAMP_NANOS = TIMESTAMP_MICROS * 1000L;

  // 22:20:38.808123 (time of day, microseconds since midnight)
  private static final long TIME_MICROS = (22 * 3600L + 20 * 60L + 38) * 1_000_000L + 808_123L;

  @Test
  public void testTimestampMicrosConverterSchemaType() {
    TimestampMicrosConverter converter = new TimestampMicrosConverter();
    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
    assertThrows(Exception.class, () -> converter.checkEncodingType(Schema.Type.INT32));
  }

  @Test
  public void testTimestampMicrosConversion() {
    TimestampMicrosConverter converter = new TimestampMicrosConverter();
    assertEquals("2017-03-01 22:20:38.808123", converter.convert(TIMESTAMP_MICROS));
  }

  @Test
  public void testTimestampMicrosConversionEpoch() {
    TimestampMicrosConverter converter = new TimestampMicrosConverter();
    assertEquals("1970-01-01 00:00:00.000000", converter.convert(0L));
  }

  @Test
  public void testTimestampNanosConverterSchemaType() {
    TimestampNanosConverter converter = new TimestampNanosConverter();
    assertEquals(LegacySQLTypeName.TIMESTAMP, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
  }

  @Test
  public void testTimestampNanosConversion() {
    TimestampNanosConverter converter = new TimestampNanosConverter();
    // nanos truncated to micros precision
    assertEquals("2017-03-01 22:20:38.808123", converter.convert(TIMESTAMP_NANOS));
  }

  @Test
  public void testTimestampNanosConversionTruncation() {
    TimestampNanosConverter converter = new TimestampNanosConverter();
    // sub-microsecond nanos are truncated, not rounded
    long nanosWithSubMicro = TIMESTAMP_MICROS * 1000L + 999L;
    assertEquals("2017-03-01 22:20:38.808123", converter.convert(nanosWithSubMicro));
  }

  @Test
  public void testLocalTimestampMillisConverterSchemaType() {
    LocalTimestampMillisConverter converter = new LocalTimestampMillisConverter();
    assertEquals(LegacySQLTypeName.DATETIME, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
  }

  @Test
  public void testLocalTimestampMillisConversion() {
    LocalTimestampMillisConverter converter = new LocalTimestampMillisConverter();
    long millis = TIMESTAMP_MICROS / 1000L;
    assertEquals("2017-03-01 22:20:38.808", converter.convert(millis));
  }

  @Test
  public void testLocalTimestampMicrosConverterSchemaType() {
    LocalTimestampMicrosConverter converter = new LocalTimestampMicrosConverter();
    assertEquals(LegacySQLTypeName.DATETIME, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
  }

  @Test
  public void testLocalTimestampMicrosConversion() {
    LocalTimestampMicrosConverter converter = new LocalTimestampMicrosConverter();
    assertEquals("2017-03-01 22:20:38.808123", converter.convert(TIMESTAMP_MICROS));
  }

  @Test
  public void testLocalTimestampNanosConverterSchemaType() {
    LocalTimestampNanosConverter converter = new LocalTimestampNanosConverter();
    assertEquals(LegacySQLTypeName.DATETIME, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
  }

  @Test
  public void testLocalTimestampNanosConversion() {
    LocalTimestampNanosConverter converter = new LocalTimestampNanosConverter();
    assertEquals("2017-03-01 22:20:38.808123", converter.convert(TIMESTAMP_NANOS));
  }

  @Test
  public void testTimeMicrosConverterSchemaType() {
    TimeMicrosConverter converter = new TimeMicrosConverter();
    assertEquals(LegacySQLTypeName.TIME, converter.getBqSchemaType());
    converter.checkEncodingType(Schema.Type.INT64);
    assertThrows(Exception.class, () -> converter.checkEncodingType(Schema.Type.INT32));
  }

  @Test
  public void testTimeMicrosConversion() {
    TimeMicrosConverter converter = new TimeMicrosConverter();
    assertEquals("22:20:38.808123", converter.convert(TIME_MICROS));
  }

  @Test
  public void testTimeMicrosConversionMidnight() {
    TimeMicrosConverter converter = new TimeMicrosConverter();
    assertEquals("00:00:00.000000", converter.convert(0L));
  }
}
