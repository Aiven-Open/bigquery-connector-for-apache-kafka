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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StorageApiBatchModeHandlerTest {
  StorageWriteApiBatchApplicationStream mockedStreamApi = mock(StorageWriteApiBatchApplicationStream.class);
  BigQuerySinkTaskConfig mockedConfig = mock(BigQuerySinkTaskConfig.class);
  Map<TopicPartition, OffsetAndMetadata> offsetInfo = new HashMap<>();
  StorageApiBatchModeHandler batchModeHandler = new StorageApiBatchModeHandler(
      mockedStreamApi,
      mockedConfig
  );
  List<ConvertedRecord> rows = new ArrayList<>();

  @BeforeEach
  public void setup() {
    when(mockedConfig.getString(BigQuerySinkTaskConfig.PROJECT_CONFIG)).thenReturn("p");
    when(mockedConfig.getString(BigQuerySinkTaskConfig.DEFAULT_DATASET_CONFIG)).thenReturn("d1");
    when(mockedConfig.getBoolean(BigQuerySinkTaskConfig.SANITIZE_TOPICS_CONFIG)).thenReturn(false);
    when(mockedConfig.getList(BigQuerySinkTaskConfig.TOPICS_CONFIG)).thenReturn(
        Arrays.asList("topic1", "topic2")
    );
    when(mockedStreamApi.maybeCreateStream(any(), any())).thenReturn(true);
    when(mockedStreamApi.updateOffsetsOnStream(any(), any())).thenReturn("s1_app_stream");
    when(mockedStreamApi.getCommitableOffsets()).thenReturn(offsetInfo);
  }

  @Test
  public void testCommitStreams() {
    batchModeHandler.refreshStreams();
  }

  @Test
  public void testUpdateOffsetsOnStream() {
    String actualStreamName = batchModeHandler.updateOffsetsOnStream(
        TableName.of("p", "d1", "topic1").toString(), rows);

    assertEquals("s1_app_stream", actualStreamName);
    verify(mockedStreamApi, times(1))
        .updateOffsetsOnStream("projects/p/datasets/d1/tables/topic1", rows);
  }

  @Test
  public void testGetCommitableOffsets() {
    batchModeHandler.getCommitableOffsets();
    verify(mockedStreamApi, times(1)).getCommitableOffsets();
  }
}
