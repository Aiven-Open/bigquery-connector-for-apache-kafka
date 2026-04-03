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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Resolves the Kafka consumer {@link TopicPartition} and offset for Connect offset commits.
 * After SMTs that rename topics (e.g. RegexRouter), {@link SinkRecord#topic()} no longer matches
 * the assigned partition; Connect 3.6+ exposes {@link SinkRecord#originalTopic()} and related
 * fields for this case (see {@code SinkRecord} Javadoc).
 */
public final class SinkRecordConnectOffsets {

  private SinkRecordConnectOffsets() {
  }

  /**
   * Topic and partition as assigned on the consumer, before any transformation.
   */
  public static TopicPartition topicPartitionForCommit(SinkRecord record) {
    String topic;
    Integer partition;
    try {
      topic = record.originalTopic();
      partition = record.originalKafkaPartition();
    } catch (NoSuchMethodError e) {
      topic = record.topic();
      partition = record.kafkaPartition();
    }
    if (topic == null) {
      topic = record.topic();
    }
    if (partition == null) {
      partition = record.kafkaPartition();
    }
    if (topic == null || partition == null) {
      throw new IllegalStateException("SinkRecord missing topic or partition for offset commit");
    }
    return new TopicPartition(topic, partition);
  }

  /**
   * Next offset to resume after this record (exclusive), i.e. {@code originalKafkaOffset + 1} when available.
   */
  public static long nextOffsetExclusiveForCommit(SinkRecord record) {
    try {
      return record.originalKafkaOffset() + 1;
    } catch (NoSuchMethodError e) {
      return record.kafkaOffset() + 1;
    }
  }
}
