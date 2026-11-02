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

import java.util.Comparator;
import java.util.Objects;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A record that is used to track offsets from Kafka. Generally used in maps to track source or sink
 * records.
 *
 * @param topic the original topic.
 * @param partition the original partition.
 * @param offset the original offset.
 */
public record KafkaRecordKey(String topic, int partition, long offset)
    implements Comparable<KafkaRecordKey> {

  /**
   * Creates a KafkaRecordKey with the maximum value.
   *
   * @param topicPartition the topic partition to create the max key for.
   * @return a KafkaRecordKey with the maximum value.
   */
  public static KafkaRecordKey maxKey(TopicPartition topicPartition) {
    return new KafkaRecordKey(topicPartition, Long.MAX_VALUE);
  }

  /**
   * Creates a KafkaRecordKey with the maximum value.
   *
   * @param topic the topic to create the max key for.
   * @param partition the partition to create the max key for.
   * @return a KafkaRecordKey with the maximum value.
   */
  public static KafkaRecordKey maxKey(String topic, int partition) {
    return new KafkaRecordKey(topic, partition, Long.MAX_VALUE);
  }

  /**
   * Creates a KafkaRecordKey with the minimum value.
   *
   * @param topicPartition the topic partition to create the nin key for.
   * @return a KafkaRecordKey with the maximum value.
   */
  public static KafkaRecordKey minKey(TopicPartition topicPartition) {
    return new KafkaRecordKey(topicPartition, 0);
  }

  /**
   * Creates a KafkaRecordKey with the minimum value.
   *
   * @param topic the topic to create the min key for.
   * @param partition the partition to create the min key for.
   * @return a KafkaRecordKey with the minimum value.
   */
  public static KafkaRecordKey minKey(String topic, int partition) {
    return new KafkaRecordKey(topic, partition, 0);
  }

  /**
   * Creates an instance from a SinkRecord.
   *
   * @param sinkRecord the sink record to extract data from.
   */
  public KafkaRecordKey(SinkRecord sinkRecord) {
    this(
        sinkRecord.originalTopic(),
        sinkRecord.originalKafkaPartition(),
        sinkRecord.originalKafkaOffset());
  }

  /**
   * Creates an instance from a RecordMetadata instance.
   *
   * @param recordMetadata the record metadata to extract data from.
   */
  public KafkaRecordKey(RecordMetadata recordMetadata) {
    this(recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
  }

  /**
   * Creates an instance from a TopicPartition object and an offset.
   *
   * @param topicPartition the TopicPartition to use.
   * @param offset the offset.
   */
  public KafkaRecordKey(TopicPartition topicPartition, long offset) {
    this(topicPartition.topic(), topicPartition.partition(), offset);
  }

  /**
   * Gets the TopicPartition for this key.
   *
   * @return the TopicPartition for this key.
   */
  public TopicPartition getTopicPartition() {
    return new TopicPartition(topic, partition);
  }

  /**
   * Gets the OffsetAndMetadata for this key.
   *
   * @return the OffsetAndMetadata for this key.
   */
  public OffsetAndMetadata getOffsetAndMetadata() {
    return new OffsetAndMetadata(offset);
  }

  @Override
  public int compareTo(KafkaRecordKey that) {
    return Objects.compare(
        this,
        that,
        Comparator.comparing(KafkaRecordKey::topic)
            .thenComparing(KafkaRecordKey::partition)
            .thenComparing(KafkaRecordKey::offset));
  }

  @Override
  public String toString() {
    return String.format("Kafka record[t:%s p:%s o:%s]", topic, partition, offset);
  }
}
