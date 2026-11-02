/*
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
import org.apache.kafka.connect.sink.SinkTask;

/** Defines the commit strategy for the writers. */
public interface CommitStrategy {

  /**
   * Associates the rows with the stream container. When the streamContainer indicates that it is
   * commited or inactive then the rows are committed and Kafka may commit them.
   *
   * @param rows the collection rows to add to the queue of waiting rows.
   * @param streamStateContainer the container that will write the rows.
   */
  default void queue(Collection<SinkRecord> rows, StreamStateContainer streamStateContainer) {
    queue(rows.stream(), streamStateContainer);
  }

  /**
   * Associates the rows with the stream container. When the streamContainer indicates that it is
   * commited or inactive then the rows are committed and Kafka may commit them.
   *
   * @param rows the stream of rows to add to the queue of waiting rows.
   * @param streamStateContainer the container that will write the rows.
   */
  void queue(Stream<SinkRecord> rows, StreamStateContainer streamStateContainer);

  /**
   * Execute the equivalent of the {@link SinkTask#preCommit(Map)}.
   *
   * @param offsets the offsets to check.
   * @return a map of topicPartition to offsets that may be commited in Kafka.
   */
  Map<TopicPartition, OffsetAndMetadata> preCommit(
      final Map<TopicPartition, OffsetAndMetadata> offsets);

  /**
   * Clean up any internal records that are no longer needed. This method will be called
   * periodically to allow the commit strategy to remove any records that are no longer needed but
   * that have been removed during normal processing. When this method returns {@code false} it is
   * removed from the scheduler and will not be run again. This allows CommitStrategies that do not
   * need the clean to be called to stop the automatic rescheduling.
   *
   * @return {@code true} if the commit strategy should be called again, {@code false} otherwise.
   */
  boolean cleanCommitStrategy();

  /**
   * Begins tracking the specified {@code TopicPartition}s.
   *
   * @param partitions the {@code TopicPartition}s to begin tracking.
   */
  void registerPartitions(Collection<TopicPartition> partitions);

  /**
   * Stops tracking the specified {@code TopicPartition}. Any {@link StreamStateContainer}s that
   * contain the queued records for the specified {@code TopicPartition} should be aborted and not
   * reported.
   *
   * @param partitions the {@code TopicPartition} to stop tracking.
   */
  void unregisterPartitions(Collection<TopicPartition> partitions);
}
