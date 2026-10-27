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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

  private static final Predicate<StreamStateContainer> CAN_COMMIT =
      streamStateContainer -> streamStateContainer.stateIs(StreamState.COMMITTED);

  private static final Predicate<StreamStateContainer> CAN_REMOVE =
      streamStateContainer -> streamStateContainer.stateIs(StreamState.INACTIVE);

  /** The map of KafkaRecordKeys to Trackers */
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, StreamStateContainer> commitMap;

  private final ConcurrentSkipListMap<TopicPartition, Set<StreamStateContainer>> partitions;

  public HighestUnblockedCommitStrategy() {
    commitMap = new ConcurrentSkipListMap<>();
    partitions =
        new ConcurrentSkipListMap<>(
            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));
  }

  @Override
  public void queue(
      final Stream<SinkRecord> rows, final StreamStateContainer streamStateContainer) {
    rows.map(KafkaRecordKey::new)
        .forEach(
            key -> {
              commitMap.put(key, streamStateContainer);
              partitions.compute(
                  key.getTopicPartition(),
                  (k, v) -> {
                    if (v == null) {
                      v = new HashSet<>();
                    }
                    v.add(streamStateContainer);
                    return v;
                  });
            });
  }

  /**
   * Create a sub map of the commitMap that is in the range 0 to key.offset in for the topic and
   * partition specified by the key.
   *
   * @param key the KafkaRecordKey that is the upper limit for proposed committable records.
   * @return the sub map of the commitMap that is in the range 0 to key.offset in for the topic and
   *     partition specified.
   */
  @VisibleForTesting
  ConcurrentNavigableMap<KafkaRecordKey, StreamStateContainer> createSubMap(KafkaRecordKey key) {
    return commitMap.subMap(KafkaRecordKey.minKey(key.topic(), key.partition()), true, key, true);
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
    boolean sawBreak = false;
    for (Map.Entry<KafkaRecordKey, StreamStateContainer> entry : subMap.entrySet()) {
      if (entry.getValue().stateIs(StreamState.COMMITTED)) {
        subMap.remove(entry.getKey());
        if (!sawBreak) {
          highestCommitableKey = entry.getKey();
        }
      } else if (entry.getValue().stateIs(StreamState.INACTIVE)) {
        // inactive should already have been removed, but just in case.
        subMap.remove(entry.getKey());
        partitions.computeIfPresent(
            entry.getKey().getTopicPartition(),
            (k, v) -> {
              v.remove(entry.getValue());
              return v;
            });
      } else {
        sawBreak = true;
      }
    }
    return highestCommitableKey;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // get the set of committed containers.
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    partitions.entrySet().stream()
        .filter(entry -> entry.getValue().stream().anyMatch(CAN_COMMIT))
        .map(Map.Entry::getKey)
        .forEach(
            topicPartition -> {
              KafkaRecordKey highestCommittableKey =
                  getHighestCommittableKey(createSubMap(KafkaRecordKey.maxKey(topicPartition)));
              if (highestCommittableKey != null) {
                result.put(topicPartition, highestCommittableKey.getOffsetAndMetadata());
              }
            });
    return result;
  }

  @Override
  public boolean cleanCommitStrategy() {
    logger.debug("Cleaning commit map: {} records", commitMap.size());
    int removedRecords = 0;
    Map.Entry<KafkaRecordKey, StreamStateContainer> entry = commitMap.firstEntry();
    while (entry != null) {
      if (entry.getValue().stateIs(StreamState.INACTIVE)) {
        commitMap.remove(entry.getKey());
        removedRecords++;
      }
      entry = commitMap.higherEntry(entry.getKey());
    }
    logger.debug("Cleaned commit map: {} records, removed {}", commitMap.size(), removedRecords);

    logger.debug("Cleaning commit partitions");
    partitions.values().forEach(x -> x.removeIf(CAN_REMOVE));
    return true;
  }

  @Override
  public void registerPartitions(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> this.partitions.computeIfAbsent(topicPartition, k -> new HashSet<>()));
    if (logger.isDebugEnabled()) {
      logger.debug("Registered partition count: {}", this.partitions.size());
    }
  }

  @Override
  public void unregisterPartitions(Collection<TopicPartition> partitions) {
    // have to stop writing to any open streams for the partition and abort the write.
    final Set<StreamStateContainer> containers = new HashSet<>();
    for (TopicPartition topicPartition : partitions) {
      // figure out how to report records commited but not reported to Kafka
      Map<KafkaRecordKey, StreamStateContainer> subMap =
          createSubMap(KafkaRecordKey.maxKey(topicPartition));
      subMap.forEach(
          (k, v) -> {
            if (containers.add(v)) {
              v.abort();
            }
          });
      subMap.clear();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Registered partition count: {}", this.partitions.size());
    }
  }
}
