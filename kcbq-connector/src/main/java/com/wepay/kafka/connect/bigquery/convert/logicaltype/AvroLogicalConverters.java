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

import com.google.cloud.bigquery.LegacySQLTypeName;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

/**
 * Class containing converters for Avro logical time types that are passed through as named
 * INT64 schemas by the Confluent schema registry AvroData converter.
 */
public class AvroLogicalConverters {

  static final String AVRO_LOGICAL_TIMESTAMP_MICROS = "timestamp-micros";
  static final String AVRO_LOGICAL_TIMESTAMP_NANOS = "timestamp-nanos";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";
  static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS = "local-timestamp-nanos";
  static final String AVRO_LOGICAL_TIME_MICROS = "time-micros";

  private static final int MICROS_IN_MILLI = 1000;
  private static final long MICROS_IN_SEC = 1_000_000L;
  private static final long NANOS_IN_MICRO = 1000L;

  public static void initialize() {
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_TIMESTAMP_MICROS, new TimestampMicrosConverter());
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_TIMESTAMP_NANOS, new TimestampNanosConverter());
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS, new LocalTimestampMillisConverter());
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS, new LocalTimestampMicrosConverter());
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS, new LocalTimestampNanosConverter());
    LogicalConverterRegistry.registerIfAbsent(AVRO_LOGICAL_TIME_MICROS, new TimeMicrosConverter());
  }

  public static void remove() {
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_TIMESTAMP_MICROS);
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_TIMESTAMP_NANOS);
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS);
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS);
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS);
    LogicalConverterRegistry.unregister(AVRO_LOGICAL_TIME_MICROS);
  }

  private AvroLogicalConverters() {
    // do not instantiate.
  }

  /**
   * Converts Avro timestamp-micros (INT64, microseconds since epoch) to BigQuery TIMESTAMP.
   */
  public static class TimestampMicrosConverter extends LogicalTypeConverter {
    public TimestampMicrosConverter() {
      super(AVRO_LOGICAL_TIMESTAMP_MICROS, Schema.Type.INT64, LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long microTimestamp = (Long) kafkaConnectObject;
      return formatMicrosAsTimestamp(microTimestamp);
    }
  }

  /**
   * Converts Avro timestamp-nanos (INT64, nanoseconds since epoch) to BigQuery TIMESTAMP.
   * Nanosecond precision is truncated to microseconds since BigQuery TIMESTAMP supports up to micros.
   */
  public static class TimestampNanosConverter extends LogicalTypeConverter {
    public TimestampNanosConverter() {
      super(AVRO_LOGICAL_TIMESTAMP_NANOS, Schema.Type.INT64, LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long nanoTimestamp = (Long) kafkaConnectObject;
      long microTimestamp = nanoTimestamp / NANOS_IN_MICRO;
      return formatMicrosAsTimestamp(microTimestamp);
    }
  }

  /**
   * Converts Avro local-timestamp-millis (INT64, milliseconds since epoch, no timezone) to BigQuery DATETIME.
   */
  public static class LocalTimestampMillisConverter extends LogicalTypeConverter {
    public LocalTimestampMillisConverter() {
      super(AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS, Schema.Type.INT64, LegacySQLTypeName.DATETIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long milliTimestamp = (Long) kafkaConnectObject;
      Date date = new Date(milliTimestamp);
      SimpleDateFormat bqDatetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      bqDatetimeFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
      return bqDatetimeFormat.format(date);
    }
  }

  /**
   * Converts Avro local-timestamp-micros (INT64, microseconds since epoch, no timezone) to BigQuery DATETIME.
   */
  public static class LocalTimestampMicrosConverter extends LogicalTypeConverter {
    public LocalTimestampMicrosConverter() {
      super(AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS, Schema.Type.INT64, LegacySQLTypeName.DATETIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long microTimestamp = (Long) kafkaConnectObject;
      return formatMicrosAsDatetime(microTimestamp);
    }
  }

  /**
   * Converts Avro local-timestamp-nanos (INT64, nanoseconds since epoch, no timezone) to BigQuery DATETIME.
   * Nanosecond precision is truncated to microseconds since BigQuery DATETIME supports up to micros.
   */
  public static class LocalTimestampNanosConverter extends LogicalTypeConverter {
    public LocalTimestampNanosConverter() {
      super(AVRO_LOGICAL_LOCAL_TIMESTAMP_NANOS, Schema.Type.INT64, LegacySQLTypeName.DATETIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long nanoTimestamp = (Long) kafkaConnectObject;
      long microTimestamp = nanoTimestamp / NANOS_IN_MICRO;
      return formatMicrosAsDatetime(microTimestamp);
    }
  }

  /**
   * Converts Avro time-micros (INT64, microseconds since midnight) to BigQuery TIME.
   */
  public static class TimeMicrosConverter extends LogicalTypeConverter {
    public TimeMicrosConverter() {
      super(AVRO_LOGICAL_TIME_MICROS, Schema.Type.INT64, LegacySQLTypeName.TIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      long microTimestamp = (Long) kafkaConnectObject;
      long milliTimestamp = microTimestamp / MICROS_IN_MILLI;
      Date date = new Date(milliTimestamp);

      SimpleDateFormat bqTimeSecondsFormat = new SimpleDateFormat("HH:mm:ss");
      bqTimeSecondsFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
      String formattedSecondsTime = bqTimeSecondsFormat.format(date);

      long microRemainder = microTimestamp % MICROS_IN_SEC;
      return formattedSecondsTime + "." + String.format("%06d", microRemainder);
    }
  }

  private static String formatMicrosAsTimestamp(long microTimestamp) {
    long milliTimestamp = microTimestamp / MICROS_IN_MILLI;
    Date date = new Date(milliTimestamp);

    SimpleDateFormat bqTimestampSecondsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    bqTimestampSecondsFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
    String formattedSeconds = bqTimestampSecondsFormat.format(date);

    long microRemainder = microTimestamp % MICROS_IN_SEC;
    return formattedSeconds + "." + String.format("%06d", microRemainder);
  }

  private static String formatMicrosAsDatetime(long microTimestamp) {
    long milliTimestamp = microTimestamp / MICROS_IN_MILLI;
    Date date = new Date(milliTimestamp);

    SimpleDateFormat bqDatetimeSecondsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    bqDatetimeSecondsFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
    String formattedSeconds = bqDatetimeSecondsFormat.format(date);

    long microRemainder = microTimestamp % MICROS_IN_SEC;
    return formattedSeconds + "." + String.format("%06d", microRemainder);
  }
}
