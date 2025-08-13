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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Class containing all the Kafka logical type converters.
 */
public class KafkaLogicalConverters {

  /**
   * These values are extracted from BigQuery documentation.
   *
   * @see <a href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type">BigQuery data types</a>
   */
  private static final int MAX_NUMERIC_PRECISION = 38;
  private static final int MAX_NUMERIC_SCALE = 9;

  public static void initialize(final BigQuerySinkConfig config) {
    LogicalConverterRegistry.register(Date.LOGICAL_NAME, new DateConverter());
    LogicalConverterRegistry.register(Decimal.LOGICAL_NAME, new DecimalConverter(config.getDecimalHandlingMode()));
    LogicalConverterRegistry.register(Timestamp.LOGICAL_NAME, new TimestampConverter());
    LogicalConverterRegistry.register(Time.LOGICAL_NAME, new TimeConverter());
  }

  private KafkaLogicalConverters() {
    // do not instantiate.
  }

  /**
   * Class for converting Kafka date logical types to Bigquery dates.
   */
  public static class DateConverter extends LogicalTypeConverter {
    /**
     * Create a new DateConverter.
     */
    public DateConverter() {
      super(Date.LOGICAL_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.DATE);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqDateFormat().format((java.util.Date) kafkaConnectObject);
    }
  }

  /**
   * Class for converting Kafka decimal logical types to Bigquery floating points.
   */
  public static class DecimalConverter extends LogicalTypeConverter {
    private final BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode;

    /**
     * Create a new DecimalConverter.
     */
    public DecimalConverter(final BigQuerySinkConfig.DecimalHandlingMode decimalHandlingMode) {
      super(Decimal.LOGICAL_NAME,
              Schema.Type.BYTES,
              decimalHandlingMode.sqlTypeName);
      this.decimalHandlingMode = decimalHandlingMode;
    }

    @Override
    public Object convert(Object kafkaConnectObject) {
      if (kafkaConnectObject == null) {
        return null;
      }
      // cast to get ClassCastException
      BigDecimal decimal = (BigDecimal) kafkaConnectObject;
      switch (decimalHandlingMode) {
        case RECORD:
          Map<String, Object> struct = new HashMap<>();
          struct.put("scale", decimal.scale());
          struct.put("value", decimal.unscaledValue().toByteArray());
          return struct;
        case FLOAT:
          return decimal.doubleValue();
        case NUMERIC:
        case BIGNUMERIC:
          return decimal;
        default:
          throw new ConversionConnectException("Unsupported decimal handling mode: " + decimalHandlingMode);
      }
    }

    public Field.Builder getFieldBuilder(
            Schema schema, String fieldName, BiFunction<Schema, String, Optional<Field.Builder>> convertStruct) {
      checkEncodingType(schema.type());
      switch (decimalHandlingMode) {
        case RECORD:
          com.google.cloud.bigquery.Field scaleField =
                  Field.newBuilder("scale", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build();
          com.google.cloud.bigquery.Field valueField =
                  Field.newBuilder("value", LegacySQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build();
          return com.google.cloud.bigquery.Field.newBuilder(
                  fieldName,
                  LegacySQLTypeName.RECORD,
                  FieldList.of(scaleField, valueField));
        case FLOAT:
          return com.google.cloud.bigquery.Field.newBuilder(fieldName, LegacySQLTypeName.FLOAT);
        case BIGNUMERIC:
        case NUMERIC:
          Map<String, String> params = schema.parameters();
          Long precision = null;
          Long scale = null;
          if (params != null) {
            String precisionStr = params.get("connect.decimal.precision");
            if (precisionStr != null) {
              precision = Long.valueOf(precisionStr);
            }
            String scaleStr = params.get("scale");
            if (scaleStr != null) {
              scale = Long.valueOf(scaleStr);
            }
          }
          if (decimalHandlingMode.sqlTypeName == LegacySQLTypeName.NUMERIC) {
            if (precision != null && precision > MAX_NUMERIC_PRECISION) {
              throw new ConversionConnectException(String.format("Requested precision (%s) is too high for %s type", precision, decimalHandlingMode.sqlTypeName));
            }
            if (scale != null && scale > MAX_NUMERIC_SCALE) {
              throw new ConversionConnectException(String.format("Requested scale (%s) is too large for %s type", precision, decimalHandlingMode.sqlTypeName));
            }
          }
          com.google.cloud.bigquery.Field.Builder builder =
                  com.google.cloud.bigquery.Field.newBuilder(fieldName, decimalHandlingMode.sqlTypeName);
          if (precision != null) {
            builder.setPrecision(precision);
          }
          if (scale != null) {
            builder.setScale(scale);
          }
          builder.setType(decimalHandlingMode.sqlTypeName);
          return builder;
        default:
          throw new ConversionConnectException("Unsupported decimal handling mode: " + decimalHandlingMode);
      }
    }
  }

  /**
   * Class for converting Kafka timestamp logical types to BigQuery timestamps.
   */
  public static class TimestampConverter extends LogicalTypeConverter {
    /**
     * Create a new TimestampConverter.
     */
    public TimestampConverter() {
      super(Timestamp.LOGICAL_NAME,
          Schema.Type.INT64,
          LegacySQLTypeName.TIMESTAMP);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqTimestampFormat().format((java.util.Date) kafkaConnectObject);
    }
  }


  /**
   * Class for converting Kafka time logical types to BigQuery time types.
   */
  public static class TimeConverter extends LogicalTypeConverter {
    /**
     * Create a new TimestampConverter.
     */
    public TimeConverter() {
      super(Time.LOGICAL_NAME,
          Schema.Type.INT32,
          LegacySQLTypeName.TIME);
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqTimeFormat().format((java.util.Date) kafkaConnectObject);
    }
  }
}
