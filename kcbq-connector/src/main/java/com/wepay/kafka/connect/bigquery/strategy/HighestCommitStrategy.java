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
 * A commit strategy that only lets Kafka commit the highest connector committed offset for a
 * topic-partition regardless of whether there is an uncommitted offset before it. For example if
 * the list of committed offsets were: {@code [1,2,3,5,6]} and Kafka wanted to commit offset 10 the
 * commit strategy would respond with offset 6 because it is the highest committed offset even
 * though 4 is not yet committed.
 *
 * <p>Use of this strategy may result in dropped data as during a restart, the earlier uncommitted
 * records will be lost if they are not committed before the restart. In the above example the
 * listed offsets are already committed to the storage but Kafka has been told that {@code
 * [1,2,3,4,5,6]} are committed. On a restart Kafka will start sending from offset 7 and potentially
 * losing the data from offset 4 if the storage was not written before the connector restarted.
 */
public class HighestCommitStrategy implements CommitStrategy {

    private static final Logger logger = LoggerFactory.getLogger(HighestCommitStrategy.class);

    private static final Predicate<StreamStateContainer> CAN_COMMIT =
      streamStateContainer -> streamStateContainer.stateIs(StreamState.COMMITTED);

  /** The map of KafkaRecordKeys to Trackers */
  @VisibleForTesting final ConcurrentSkipListMap<KafkaRecordKey, StreamStateContainer> commitMap;

  private final ConcurrentSkipListMap<TopicPartition, Set<StreamStateContainer>> partitions;

  public HighestCommitStrategy() {
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
   * @param subMap the map of records to be committed that are in the range (
   * @return the highest CommittableKey.
   */
  private KafkaRecordKey getHighestCommittableKey(
      ConcurrentNavigableMap<KafkaRecordKey, StreamStateContainer> subMap) {
    KafkaRecordKey highestCommitableKey = null;
    Map.Entry<KafkaRecordKey, StreamStateContainer> testEntry = subMap.lastEntry();
    // backup until we either don't have a key or we reach the highest committed record
    while (testEntry != null && !testEntry.getValue().stateIs(StreamState.COMMITTED)) {
      testEntry = subMap.lowerEntry(testEntry.getKey());
    }

    return testEntry == null ? null : testEntry.getKey();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // get the set of committed containers.
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    /* if there is a committed state container for each partition, find the highestCommittableKey for that partition. */
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
      logger.debug("Cleaning commit map.");
    Map.Entry<KafkaRecordKey, StreamStateContainer> entry = commitMap.firstEntry();
    while (entry != null) {
      if (entry.getValue().stateIs(StreamState.INACTIVE)) {
        commitMap.remove(entry.getKey());
      }
      entry = commitMap.higherEntry(entry.getKey());
    }
    return true;
  }

  @Override
  public void registerPartitions(Collection<TopicPartition> partitions) {
    partitions.forEach(
        topicPartition -> this.partitions.computeIfAbsent(topicPartition, k -> new HashSet<>()));
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
  }
}
