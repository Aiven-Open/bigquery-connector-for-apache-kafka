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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A commit strategy that accepts whatever Kafka says it is going to commit. This is the default for
 * the generic Kafka connector.
 *
 * <p>Use of this strategy may result in loss of data as during a restart. For example if kafka has
 * sent record for offsets {@code [1,2,3,4,5,6]} and the storage has only written records for
 * offsets {@code [1,2,3}, Kafka would ask if it was ok to commit offset 6 and would be told yes. If
 * the system restarts before {@code [4,5,6} are written to storage those records will be lost.
 */
public class DefaultKafkaCommitStrategy implements CommitStrategy {

  public DefaultKafkaCommitStrategy() {}

  public void queue(Stream<SinkRecord> rows, StreamStateContainer streamStateContainer) {}

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return currentOffsets;
  }

  @Override
  public boolean cleanCommitStrategy() {
    return false;
  }

  @Override
  public void registerPartitions(Collection<TopicPartition> partitions) {}

  @Override
  public void unregisterPartitions(Collection<TopicPartition> partitions) {}
}
