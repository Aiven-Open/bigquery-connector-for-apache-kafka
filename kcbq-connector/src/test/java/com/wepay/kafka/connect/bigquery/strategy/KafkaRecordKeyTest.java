/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/

package com.wepay.kafka.connect.bigquery.strategy;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class KafkaRecordKeyTest {
  private static final String topic = "new.Topic";
  private static final String originalTopic = "original.Topic";
  private static final int partition = 1;
  private static final int originalPartition = 2;
  private static final long offset = 1L + Integer.MAX_VALUE;
  private static final long originalOffset = 5L + Integer.MAX_VALUE;

  private SinkRecord testRecord(String originalTopic, int originalPartition, long originalOffset) {
    return new SinkRecord(
        topic,
        partition,
        Schema.STRING_SCHEMA,
        "key",
        Schema.STRING_SCHEMA,
        "value",
        offset,
        null,
        TimestampType.NO_TIMESTAMP_TYPE,
        Collections.emptyList(),
        originalTopic,
        originalPartition,
        originalOffset);
  }

  @Test
  void sinkRecordConstructorTest() {
    KafkaRecordKey underTest =
        new KafkaRecordKey(testRecord(originalTopic, originalPartition, originalOffset));
    assertThat(underTest.offset()).isEqualTo(originalOffset);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);
  }

  @Test
  void maxKeyTest() {
    KafkaRecordKey underTest = KafkaRecordKey.maxKey(originalTopic, originalPartition);
    assertThat(underTest.offset()).isEqualTo(Long.MAX_VALUE);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);

    underTest = KafkaRecordKey.maxKey(new TopicPartition(originalTopic, originalPartition));
    assertThat(underTest.offset()).isEqualTo(Long.MAX_VALUE);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);
  }

  @Test
  void minKeyTest() {
    KafkaRecordKey underTest = KafkaRecordKey.minKey(originalTopic, originalPartition);
    assertThat(underTest.offset()).isEqualTo(0L);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);

    underTest = KafkaRecordKey.minKey(new TopicPartition(originalTopic, originalPartition));
    assertThat(underTest.offset()).isEqualTo(0L);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);
  }

  @Test
  void recordMetadataConstructorTest() {
    RecordMetadata recordMetadata =
        new RecordMetadata(
            new TopicPartition(originalTopic, originalPartition), originalOffset, 0, 0, 0, 0);
    KafkaRecordKey underTest = new KafkaRecordKey(recordMetadata);
    assertThat(underTest.offset()).isEqualTo(originalOffset);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);
  }

  @Test
  void topicPartitionOffsetConstructorTest() {
    TopicPartition topicPartition = new TopicPartition(originalTopic, originalPartition);
    KafkaRecordKey underTest = new KafkaRecordKey(topicPartition, originalOffset);
    assertThat(underTest.offset()).isEqualTo(originalOffset);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);

    underTest = new KafkaRecordKey(originalTopic, originalPartition, originalOffset);
    assertThat(underTest.offset()).isEqualTo(originalOffset);
    assertThat(underTest.topic()).isEqualTo(originalTopic);
    assertThat(underTest.partition()).isEqualTo(originalPartition);
  }

  @Test
  void getTopicPartitionTest() {
    KafkaRecordKey underTest = new KafkaRecordKey(originalTopic, originalPartition, originalOffset);
    assertThat(underTest.getTopicPartition())
        .isEqualTo(new TopicPartition(originalTopic, originalPartition));
  }

  @Test
  void getOffsetAndMetadataTest() {
    KafkaRecordKey underTest = new KafkaRecordKey(originalTopic, originalPartition, originalOffset);
    assertThat(underTest.getOffsetAndMetadata()).isEqualTo(new OffsetAndMetadata(originalOffset));
  }

  @Test
  void orderingTest() {
    List<KafkaRecordKey> expected = new ArrayList<>();
    String[] topics = {"aTopic", "btopci"};
    int[] partitions = {5, 10};
    long[] offsets = {10_000L, (long) Integer.MAX_VALUE + 1};
    for (String topic : topics) {
      for (int partition : partitions) {
        expected.add(KafkaRecordKey.minKey(topic, partition));
        for (long offset : offsets) {
          expected.add(new KafkaRecordKey(topic, partition, offset));
        }
        expected.add(KafkaRecordKey.maxKey(topic, partition));
      }
    }
    List<KafkaRecordKey> actual = new ArrayList<>(expected);
    Collections.shuffle(actual);
    Collections.sort(actual);
    assertThat(actual).containsExactlyElementsOf(expected);
  }
}
