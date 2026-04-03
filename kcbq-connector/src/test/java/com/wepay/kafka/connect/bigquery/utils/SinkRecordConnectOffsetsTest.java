/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wepay.kafka.connect.bigquery.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SinkRecordConnectOffsetsTest {

  @Test
  void topicPartitionAndOffsetUseOriginalKafkaValuesAfterTopicRename() {
    String originalTopic = "pandashop.pandashop.orders";
    String routedTopic = "orders";
    long offset = 42L;
    SinkRecord record = new SinkRecord(
        routedTopic,
        0,
        null,
        null,
        Schema.BOOLEAN_SCHEMA,
        true,
        offset,
        null,
        TimestampType.NO_TIMESTAMP_TYPE,
        null,
        originalTopic,
        0,
        offset);

    assertEquals(new TopicPartition(originalTopic, 0), SinkRecordConnectOffsets.topicPartitionForCommit(record));
    assertEquals(offset + 1, SinkRecordConnectOffsets.nextOffsetExclusiveForCommit(record));
  }

  @Test
  void withoutRenameMatchesTopicAndKafkaOffset() {
    SinkRecord record = new SinkRecord("my.topic", 1, null, null, Schema.INT8_SCHEMA, (byte) 1, 9L);
    assertEquals(new TopicPartition("my.topic", 1), SinkRecordConnectOffsets.topicPartitionForCommit(record));
    assertEquals(10L, SinkRecordConnectOffsets.nextOffsetExclusiveForCommit(record));
  }
}
