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

import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.write.storage.StreamState;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A commit strategy that only lets Kafka commit the highest offset for a topic-partition that that
 * does not have an uncommitted offset above it. For example if the list of committed offsets were:
 * {@code [1,2,3,5,6]} and Kafka wanted to commit offset 6 the commit strategy would respond with
 * offset 3 because 4 is not yet committed.
 *
 * <p>Use of this strategy may result in duplicate data as during a restart. In the above example
 * the listed offsets are already committed to the storage but Kafka has been told that only {@code
 * [1,2,3]} are committed. On a restart it will send {@code [4,5,6]} again, thus duplicating offsets
 * 5 and 6, and potentially offset 4 if the storage wrote the data while the connector restarted.
 */
public class HighestUnblockedCommitStrategy implements CommitStrategy {

  private static final Logger logger =
      LoggerFactory.getLogger(HighestUnblockedCommitStrategy.class);

  /** Committed records and inactive (completed delivery) records are candidates for commit. */
  private static final Predicate<StreamStateContainer> CAN_COMMIT =
      streamStateContainer ->
          streamStateContainer.stateIs(StreamState.COMMITTED)
              || streamStateContainer.stateIs(StreamState.INACTIVE);

  private static final Predicate<StreamStateContainer> CAN_REMOVE =
      streamStateContainer -> streamStateContainer.stateIs(StreamState.INACTIVE);

  /** The map of KafkaRecordKeys to Trackers */
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, StreamStateContainer> commitMap;

  //  private final ConcurrentSkipListMap<TopicPartition, Set<StreamStateContainer>> partitions;

  public HighestUnblockedCommitStrategy() {
    commitMap = new ConcurrentSkipListMap<>();
    //    partitions =
    //        new ConcurrentSkipListMap<>(
    //
    // Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
  }

  @Override
  public void queue(
      final Stream<SinkRecord> rows, final StreamStateContainer streamStateContainer) {
    rows.map(KafkaRecordKey::new)
        .forEach(
            key -> {
              commitMap.put(key, streamStateContainer);
              //              partitions.compute(
              //                  key.getTopicPartition(),
              //                  (k, v) -> {
              //                    if (v == null) {
              //                      v = new HashSet<>();
              //                    }
              //                    v.add(streamStateContainer);
              //                    return v;
              //                  });
            });
  }

  /**
   * Create a sub map of the commitMap that is in the range 0 to Long.MAX_VALUE in for the topic and
   * partition specified.
   *
   * @param topicPartition the topicPartition to extract.
   * @return the sub map of the commitMap that is in the range 0 to key.offset in for the topic and
   *     partition specified.
   */
  @VisibleForTesting
  ConcurrentNavigableMap<KafkaRecordKey, StreamStateContainer> createSubMap(
      TopicPartition topicPartition) {
    return commitMap.subMap(
        KafkaRecordKey.minKey(topicPartition), true, KafkaRecordKey.maxKey(topicPartition), true);
  }

  /**
   * Go through the submap looking for the highest record we can commit. This process also removes
   * any committed records from the map.
   *
   * @param subMap the map of records to be committed that are in the rante (
   * @return
   */
  private KafkaRecordKey getHighestCommittableKey(
      ConcurrentNavigableMap<KafkaRecordKey, StreamStateContainer> subMap) {
    KafkaRecordKey highestCommitableKey = null;
    for (Map.Entry<KafkaRecordKey, StreamStateContainer> entry : subMap.entrySet()) {
      if (CAN_COMMIT.test(entry.getValue())) {
        subMap.remove(entry.getKey());
        highestCommitableKey = entry.getKey();
      } else {
        return highestCommitableKey;
      }
    }
    return highestCommitableKey;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // get the set of committed containers.
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    KafkaRecordKey startKey = commitMap.firstKey();
    KafkaRecordKey endKey = null;
    KafkaRecordKey highestCommittableKey = null;
    while (startKey != null) {
      endKey = KafkaRecordKey.maxKey(startKey.topic(), startKey.partition());
      highestCommittableKey = getHighestCommittableKey(createSubMap(endKey.getTopicPartition()));
      if (highestCommittableKey != null) {
        result.put(
            highestCommittableKey.getTopicPartition(),
            highestCommittableKey.getOffsetAndMetadata());
      }
      startKey = commitMap.higherKey(endKey);
    }
    return result;
  }

  @Override
  public boolean cleanCommitStrategy() {
    //    logger.debug("Cleaning commit map: {} records", commitMap.size());
    //    int removedRecords = 0;
    //    Map.Entry<KafkaRecordKey, StreamStateContainer> entry = commitMap.firstEntry();
    //    while (entry != null) {
    //      if (CAN_REMOVE.test(entry.getValue())) {
    //        commitMap.remove(entry.getKey());
    //        removedRecords++;
    //      }
    //      entry = commitMap.higherEntry(entry.getKey());
    //    }
    //    logger.debug("Cleaned commit map: {} records, removed {}", commitMap.size(),
    // removedRecords);
    //
    //    logger.debug("Cleaning commit partitions");
    //    partitions.values().forEach(x -> x.removeIf(CAN_REMOVE));
    return false;
  }

  @Override
  public void registerPartitions(Collection<TopicPartition> partitions) {
    //    partitions.forEach(
    //        topicPartition -> this.partitions.computeIfAbsent(topicPartition, k -> new
    // HashSet<>()));
    //    if (logger.isDebugEnabled()) {
    //      logger.debug("Registered partition count: {}", this.partitions.size());
    //    }
  }

  @Override
  public void unregisterPartitions(Collection<TopicPartition> partitions) {
    // have to stop writing to any open streams for the partition and abort the write.
    for (TopicPartition topicPartition : partitions) {
      Map<KafkaRecordKey, StreamStateContainer> subMap = createSubMap(topicPartition);
      subMap.values().stream().distinct().forEach(StreamStateContainer::abort);
      subMap.clear();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Registered partition count: {}", partitions.size());
    }
  }
}
