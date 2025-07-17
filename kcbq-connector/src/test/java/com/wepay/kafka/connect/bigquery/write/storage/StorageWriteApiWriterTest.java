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

package com.wepay.kafka.connect.bigquery.write.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.protobuf.ByteString;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class StorageWriteApiWriterTest {
  Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
  Schema valueSchema = SchemaBuilder.struct()
      .field("id", Schema.INT64_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("available-name", Schema.BOOLEAN_SCHEMA)
      .field("bytes_check", Schema.BYTES_SCHEMA)
      .build();

  @Test
  public void testRecordConversion() {
    StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBase.class);
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    RecordConverter<Map<String, Object>> recordConverter = new BigQueryRecordConverter(
        false,
        true
    );
    when(mockedConfig.getRecordConverter()).thenReturn(recordConverter);
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);
    TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
        mockStreamWriter, null, sinkRecordConverter, batchModeHandler);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<ConvertedRecord>> records = ArgumentCaptor.forClass(List.class);
    String expectedKafkaKey = "{\"key\":\"12345\"}";
    Set<String> expectedKeys = new HashSet<>();
    expectedKeys.add("id");
    expectedKeys.add("name");
    expectedKeys.add("available_name");
    expectedKeys.add("i_am_kafka_key");
    expectedKeys.add("i_am_kafka_record_detail");
    expectedKeys.add("bytes_check");

    Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.of("i_am_kafka_record_detail"));
    Mockito.when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
    Mockito.when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);

    builder.addRow(createRecord("abc", 100), null);
    builder.build().run();

    verify(mockStreamWriter, times(1))
        .initializeAndWriteRecords(any(), records.capture(), any());
    assertEquals(1, records.getValue().size());

    JSONObject actual = records.getValue().get(0).converted();
    assertEquals(expectedKeys, actual.keySet());

    String actualKafkaKey = actual.get("i_am_kafka_key").toString();
    assertEquals(expectedKafkaKey, actualKafkaKey);

    JSONObject recordDetails = (JSONObject) actual.get("i_am_kafka_record_detail");
    assertTrue(recordDetails.get("insertTime") instanceof Long);

    Object bytes_check = actual.get("bytes_check");
    assertTrue(bytes_check instanceof ByteString);
  }

  @Test
  public void testBatchLoadStreamName() {
    PartitionedTableId partitionedTableId = new PartitionedTableId.Builder("d", "t").setProject("p").build();
    TableName tableName = TableNameUtils.tableName(partitionedTableId.getFullTableId());
    StorageWriteApiBase mockStreamWriter = Mockito.mock(StorageWriteApiBatchApplicationStream.class);
    BigQuerySinkTaskConfig mockedConfig = Mockito.mock(BigQuerySinkTaskConfig.class);
    when(mockedConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    RecordConverter<Map<String, Object>> recordConverter = new BigQueryRecordConverter(
        false, false);
    when (mockedConfig.getRecordConverter()).thenReturn(recordConverter);
    StorageApiBatchModeHandler batchModeHandler = mock(StorageApiBatchModeHandler.class);
    SinkRecordConverter sinkRecordConverter = new SinkRecordConverter(mockedConfig, null, null);
    ArgumentCaptor<String> streamName = ArgumentCaptor.forClass(String.class);
    String expectedStreamName = tableName.toString() + "_s1";
    TableWriterBuilder builder = new StorageWriteApiWriter.Builder(
            mockStreamWriter, partitionedTableId, sinkRecordConverter, batchModeHandler);

    Mockito.when(mockedConfig.getKafkaDataFieldName()).thenReturn(Optional.empty());
    Mockito.when(mockedConfig.getKafkaKeyFieldName()).thenReturn(Optional.of("i_am_kafka_key"));
    Mockito.when(mockedConfig.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG)).thenReturn(true);
    when(batchModeHandler.updateOffsetsOnStream(any(), any())).thenReturn(expectedStreamName);

    builder.addRow(createRecord("abc", 100), null);
    builder.build().run();

    verify(mockStreamWriter, times(1))
        .initializeAndWriteRecords(any(), any(), streamName.capture());

    assertEquals(expectedStreamName, streamName.getValue());
  }

  private SinkRecord createRecord(String topic, long offset) {
    Object key = new Struct(keySchema).put("key", "12345");
    Object value = new Struct(valueSchema)
        .put("id", 1L)
        .put("name", "1")
        .put("available-name", true)
        .put("bytes_check", new byte[]{47, 48, 49});
    return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, offset);
  }
}
