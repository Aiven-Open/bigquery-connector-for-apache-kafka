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
import java.math.BigDecimal;
import java.util.Map;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Class containing all the Kafka logical type converters.
 */
public class KafkaLogicalConverters {

  static {
    LogicalConverterRegistry.register(Date.LOGICAL_NAME, new DateConverter());
    LogicalConverterRegistry.register(Decimal.LOGICAL_NAME, new DecimalConverter());
    LogicalConverterRegistry.register(Timestamp.LOGICAL_NAME, new TimestampConverter());
    LogicalConverterRegistry.register(Time.LOGICAL_NAME, new TimeConverter());
  }

  public static void initialize() {
    // forces static initialization.
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
    /**
     * Create a new DecimalConverter.
     */
    public DecimalConverter() {
      super(Decimal.LOGICAL_NAME,
          Schema.Type.BYTES,
          LegacySQLTypeName.NUMERIC);

    }

    @Override
    public BigDecimal convert(Object kafkaConnectObject) {
      // cast to get ClassCastException
      return (BigDecimal) kafkaConnectObject;
    }

    @Override
    public com.google.cloud.bigquery.Field.Builder getFieldBuilder(
        Schema schema, String fieldName) {
      checkEncodingType(schema.type());
      Map<String, String> params = schema.parameters();
      com.google.cloud.bigquery.Field.Builder builder =
          com.google.cloud.bigquery.Field.newBuilder(fieldName, LegacySQLTypeName.NUMERIC);
      if (params != null) {
        String precisionStr = params.get("connect.decimal.precision");
        if (precisionStr != null) {
          builder.setPrecision(Long.valueOf(precisionStr));
        }
        String scaleStr = params.get("scale");
        if (scaleStr != null) {
          builder.setScale(Long.valueOf(scaleStr));
        }
      }
      return builder;
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
