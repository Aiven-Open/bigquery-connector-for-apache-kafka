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

package com.wepay.kafka.connect.bigquery.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SinkRecordConverterTest {
  private static final String TOPIC = "test-topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 42L;

  private Schema keySchema;
  private Schema valueSchema;
  private Struct keyStruct;
  private Struct valueStruct;

  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;

  @BeforeEach
  public void setUp() {
    keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .build();
    keyStruct = new Struct(keySchema)
        .put("id", 123L);

    valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    valueStruct = new Struct(valueSchema)
        .put("id", 123L)
        .put("name", "Alice");

    config = mock(BigQuerySinkTaskConfig.class);
    recordConverter = new BigQueryRecordConverter(false, true);

    when(config.getRecordConverter()).thenReturn(recordConverter);
    when(config.getLong(BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG)).thenReturn(-1L);
    when(config.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG)).thenReturn(false);
    when(config.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(false);
    when(config.getKafkaKeyFieldName()).thenReturn(Optional.empty());
    when(config.getKafkaDataFieldName()).thenReturn(Optional.empty());
    when(config.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
  }

  @Test
  public void testCdcRowUpsert() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, valueSchema, valueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    // Verify root fields
    assertEquals(123L, actual.get("id"));
    assertEquals("Alice", actual.get("name"));

    // Verify CDC metadata columns
    assertEquals("UPSERT", actual.get("_CHANGE_TYPE"));
    assertEquals(String.valueOf(OFFSET), actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowDelete() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record = new SinkRecord(TOPIC, PARTITION, keySchema, keyStruct, null, null, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    Map<String, Object> actual = sinkRecordConverter.getCdcRow(record);

    // Verify root fields from key
    assertEquals(123L, actual.get("id"));
    // Since value is null, "name" should not exist or should be null
    assertTrue(!actual.containsKey("name") || actual.get("name") == null);

    // Verify CDC metadata columns
    assertEquals("DELETE", actual.get("_CHANGE_TYPE"));
    assertEquals(String.valueOf(OFFSET), actual.get("_CHANGE_SEQUENCE_NUMBER"));
  }

  @Test
  public void testCdcRowDeleteThrowsIfKeyNull() {
    when(config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

    SinkRecord record = new SinkRecord(TOPIC, PARTITION, null, null, valueSchema, valueStruct, OFFSET);

    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(config, null, null);
    assertThrows(ConnectException.class, () -> sinkRecordConverter.getCdcRow(record));
  }
}
