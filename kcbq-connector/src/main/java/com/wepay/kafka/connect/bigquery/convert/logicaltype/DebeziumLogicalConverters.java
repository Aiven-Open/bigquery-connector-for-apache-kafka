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
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Class containing all the Debezium logical type converters.
 */
public class DebeziumLogicalConverters {

  private static final int MICROS_IN_SEC = 1000000;
  private static final int MICROS_IN_MILLI = 1000;

  public static void initialize(final BigQuerySinkConfig config) {
    LogicalConverterRegistry.register(Date.SCHEMA_NAME, new DateConverter());
    LogicalConverterRegistry.register(MicroTime.SCHEMA_NAME, new MicroTimeConverter());
    LogicalConverterRegistry.register(MicroTimestamp.SCHEMA_NAME, new MicroTimestampConverter());
    LogicalConverterRegistry.register(Time.SCHEMA_NAME, new TimeConverter());
    LogicalConverterRegistry.register(ZonedTimestamp.SCHEMA_NAME, new ZonedTimestampConverter());
    LogicalConverterRegistry.register(Timestamp.SCHEMA_NAME, new TimestampConverter(config.getShouldConvertDebeziumTimestampToInteger()));
    LogicalConverterRegistry.registerIfAbsent(VariableScaleDecimal.LOGICAL_NAME, new VariableScaleDecimalConverter(config.getVariableScaleDecimalHandlingMode()));
  }

  private DebeziumLogicalConverters() {
    // do not instantiate.
  }

  /**
   * Class for converting Debezium date logical types to BigQuery dates.
   */
  public static class DateConverter extends LogicalTypeConverter {
    /**
     * Create a new DateConverter.
     */
    public DateConverter() {
      super(Date.SCHEMA_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.DATE);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      Integer daysSinceEpoch = (Integer) kafkaConnectObject;
      long msSinceEpoch = TimeUnit.DAYS.toMillis(daysSinceEpoch);
      java.util.Date date = new java.util.Date(msSinceEpoch);
      return getBqDateFormat().format(date);
    }
  }

  /**
   * Class for converting Debezium micro time logical types to BigQuery times.
   */
  public static class MicroTimeConverter extends LogicalTypeConverter {
    /**
     * Create a new MicroTimeConverter.
     */
    public MicroTimeConverter() {
      super(MicroTime.SCHEMA_NAME,
          Schema.Type.INT64,
          LegacySQLTypeName.TIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      // We want to maintain the micro second info, but date only supports up to milli.
      Long microTimestamp = (Long) kafkaConnectObject;

      Long milliTimestamp = microTimestamp / MICROS_IN_MILLI;
      java.util.Date date = new java.util.Date(milliTimestamp);

      SimpleDateFormat bqTimeSecondsFormat = new SimpleDateFormat("HH:mm:ss");
      bqTimeSecondsFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
      String formattedSecondsTimestamp = bqTimeSecondsFormat.format(date);

      Long microRemainder = microTimestamp % MICROS_IN_SEC;

      return formattedSecondsTimestamp + "." + String.format("%06d", microRemainder);
    }
  }

  /**
   * Class for converting Debezium micro timestamp logical types to BigQuery datetimes.
   */
  public static class MicroTimestampConverter extends LogicalTypeConverter {
    /**
     * Create a new MicroTimestampConverter.
     */
    public MicroTimestampConverter() {
      super(MicroTimestamp.SCHEMA_NAME,
          Schema.Type.INT64,
          LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      // We want to maintain the micro second info, but date only supports up to milli.
      Long microTimestamp = (Long) kafkaConnectObject;

      Long milliTimestamp = microTimestamp / MICROS_IN_MILLI;
      java.util.Date date = new java.util.Date(milliTimestamp);

      SimpleDateFormat bqDatetimeSecondsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      bqDatetimeSecondsFormat.setTimeZone(LogicalTypeConverter.utcTimeZone);
      String formattedSecondsTimestamp = bqDatetimeSecondsFormat.format(date);

      Long microRemainder = microTimestamp % MICROS_IN_SEC;

      return formattedSecondsTimestamp + "." + String.format("%06d", microRemainder);
    }
  }

  /**
   * Class for converting Debezium time logical types to BigQuery times.
   */
  public static class TimeConverter extends LogicalTypeConverter {
    /**
     * Create a new TimeConverter.
     */
    public TimeConverter() {
      super(Time.SCHEMA_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.TIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      java.util.Date date = new java.util.Date((Integer) kafkaConnectObject);
      return getBqTimeFormat().format(date);
    }
  }

  /**
   * Class for converting Debezium timestamp logical types to BigQuery timestamps.
   */
  public static class TimestampConverter extends LogicalTypeConverter {
    private final boolean asInteger;

    /**
     * Create a new TimestampConverter.
     */
    public TimestampConverter(boolean asInteger) {
      super(Timestamp.SCHEMA_NAME,
          Schema.Type.INT64,
          LegacySQLTypeName.TIMESTAMP);
      this.asInteger = asInteger;
    }

    @Override
    public Object convert(Object kafkaConnectObject) {
      if (asInteger) {
        return (Long) kafkaConnectObject;
      }
      java.util.Date date = new java.util.Date((Long) kafkaConnectObject);
      return getBqTimestampFormat().format(date);
    }
  }

  /**
   * Class for converting Debezium zoned timestamp logical types to BigQuery timestamps.
   */
  public static class ZonedTimestampConverter extends LogicalTypeConverter {
    /**
     * Create a new ZoneTimestampConverter.
     */
    public ZonedTimestampConverter() {
      super(ZonedTimestamp.SCHEMA_NAME,
          Schema.Type.STRING,
          LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      TemporalAccessor parsedTime = ZonedTimestamp.FORMATTER.parse((String) kafkaConnectObject);
      DateTimeFormatter bqZonedTimestampFormat =
          new DateTimeFormatterBuilder()
              .append(DateTimeFormatter.ISO_LOCAL_DATE)
              .appendLiteral(' ')
              .append(DateTimeFormatter.ISO_TIME)
              .toFormatter();
      return bqZonedTimestampFormat.format(parsedTime);
    }
  }

  /**
   * Class for converting Debezium variable scale decimals.
   */
  public static class VariableScaleDecimalConverter extends LogicalTypeConverter {
    private final BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode;

    /**
     * Create a new VariableScaleDecimalConverter.
     */
    public VariableScaleDecimalConverter(final BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode) {
      super(VariableScaleDecimal.LOGICAL_NAME,
          Schema.Type.STRUCT,
          LegacySQLTypeName.NUMERIC);
      this.decimalHandlingMode = decimalHandlingMode;
    }

    @Override
    public Object convert(Object kafkaConnectObject) {
      if (kafkaConnectObject == null) {
        return null;
      }
      // may throw class cast exception.
      Struct struct = (Struct) kafkaConnectObject;
      switch (decimalHandlingMode) {
        case RECORD:
          return kafkaConnectObject;
        case FLOAT:
          return VariableScaleDecimal.toLogical(struct).doubleValue();
        case NUMERIC:
        case BIGNUMERIC:
        default:
          return VariableScaleDecimal.toLogical(struct);
      }
    }
  }
}
