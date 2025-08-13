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
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import java.text.SimpleDateFormat;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Schema;

/**
 * Abstract class for logical type converters.
 * Contains logic for both schema and record conversions.
 */
public abstract class LogicalTypeConverter {

  // BigQuery uses UTC timezone by default
  protected static final TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");

  private final String logicalName;
  private final Schema.Type encodingType;
  private final LegacySQLTypeName bqSchemaType;

  /**
   * Create a new LogicalConverter.
   *
   * @param logicalName  The name of the logical type.
   * @param encodingType The encoding type of the logical type.
   * @param bqSchemaType The corresponding BigQuery Schema type of the logical type.
   */
  public LogicalTypeConverter(String logicalName,
                              Schema.Type encodingType,
                              LegacySQLTypeName bqSchemaType) {
    this.logicalName = logicalName;
    this.encodingType = encodingType;
    this.bqSchemaType = bqSchemaType;
  }

  protected static SimpleDateFormat getBqTimestampFormat() {
    SimpleDateFormat bqTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    bqTimestampFormat.setTimeZone(utcTimeZone);
    return bqTimestampFormat;
  }

  protected static SimpleDateFormat getBqDateFormat() {
    SimpleDateFormat bqDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    bqDateFormat.setTimeZone(utcTimeZone);
    return bqDateFormat;
  }

  protected static SimpleDateFormat getBqTimeFormat() {
    SimpleDateFormat bqTimeFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    bqTimeFormat.setTimeZone(utcTimeZone);
    return bqTimeFormat;
  }

  /**
   * @param encodingType the encoding type to check.
   * @throws ConversionConnectException if the given schema encoding type is not the same as the
   *                                    expected encoding type.
   */
  public void checkEncodingType(Schema.Type encodingType) throws ConversionConnectException {
    if (encodingType != this.encodingType) {
      throw new ConversionConnectException(
          "Logical Type " + logicalName + " must be encoded as " + this.encodingType + "; "
              + "instead, found " + encodingType
      );
    }
  }

  public LegacySQLTypeName getBqSchemaType() {
    return bqSchemaType;
  }

  /**
   * Build a BigQuery field for the given Kafka Connect schema.
   * Subclasses may override to customize precision, scale, or type.
   *
   * @param schema the Kafka Connect schema of the logical field
   * @param fieldName the name of the field
   * @return a {@link Field.Builder} initialized for this logical type
   * @deprecated use {@link #getFieldBuilder(Schema, String, BiFunction)}
   */
  @Deprecated
  public Field.Builder getFieldBuilder(Schema schema, String fieldName) {
    return getFieldBuilder(schema, fieldName, (a, b) -> Optional.empty());
  }

  /**
   * Build a BigQuery field for the given Kafka Connect schema.
   * Subclasses may override to customize precision, scale, or type.
   *
   * @param schema the Kafka Connect schema of the logical field
   * @param fieldName the name of the field
   * @param convertStruct a function that converts the schema and field name into a Field.Builder.
   * @return a {@link Field.Builder} initialized for this logical type
   */
  public Field.Builder getFieldBuilder(Schema schema, String fieldName, BiFunction<Schema, String, Optional<Field.Builder>> convertStruct) {
    checkEncodingType(schema.type());
    return Field.newBuilder(fieldName, bqSchemaType);
  }


  /**
   * Convert the given KafkaConnect Record Object to a BigQuery Record Object.
   *
   * @param kafkaConnectObject the kafkaConnectObject
   * @return the converted Object
   */
  public abstract Object convert(Object kafkaConnectObject);
}
