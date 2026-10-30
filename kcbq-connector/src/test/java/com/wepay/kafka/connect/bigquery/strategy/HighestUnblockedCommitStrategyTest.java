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

import com.wepay.kafka.connect.bigquery.write.storage.StreamState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HighestUnblockedCommitStrategyTest {
  private static final String topic = "new.Topic";
  private static final int partition = 1;
  private static final long offset = 1L + Integer.MAX_VALUE;
  private static final Random random = new Random();

  private HighestUnblockedCommitStrategy underTest;

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
        Math.abs(originalOffset));
  }

  @BeforeEach
  void setup() {
    underTest = new HighestUnblockedCommitStrategy();
  }

  @Test
  void holesStopCommitTest() {
    List<TestContainer> containers = new ArrayList<>();
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 10)), containers.get(0));
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 12)), containers.get(1));
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 13)), containers.get(2));

    Map<TopicPartition, OffsetAndMetadata> actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).isEmpty();

    for (TestContainer c : containers) {
      c.state = StreamState.APPEND;
    }
    actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).isEmpty();

    containers.get(0).state = StreamState.COMMITTED;
    containers.get(2).state = StreamState.COMMITTED;
    actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).hasSize(1).contains(new TestEntry("aTopic", 2, 10));
    assertThat(underTest.commitMap).hasSize(2);

    containers.get(1).state = StreamState.COMMITTED;
    actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).hasSize(1).contains(new TestEntry("aTopic", 2, 13));
  }

  @Test
  void inactiveDoesNotBlockCommit() {
    List<TestContainer> containers = new ArrayList<>();
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 10)), containers.get(0));
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 12)), containers.get(1));
    containers.add(new TestContainer());
    underTest.queue(List.of(testRecord("aTopic", 2, 13)), containers.get(2));

    Map<TopicPartition, OffsetAndMetadata> actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).isEmpty();

    for (TestContainer c : containers) {
      c.state = StreamState.APPEND;
    }
    containers.get(0).state = StreamState.INACTIVE;
    containers.get(1).state = StreamState.COMMITTED;
    containers.get(2).state = StreamState.APPEND;
    actual = underTest.preCommit(Collections.emptyMap());
    assertThat(actual).hasSize(1).contains(new TestEntry("aTopic", 2, 12));
  }

  private TestContainer register(String topic, int partition) {
    TestContainer testContainer = new TestContainer();
    underTest.queue(
        List.of(
            testRecord(topic, partition, random.nextLong()),
            testRecord(topic, partition, random.nextLong()),
            testRecord(topic, partition, random.nextLong())),
        testContainer);
    return testContainer;
  }

  private int partitionCount() {
    KafkaRecordKey startKey = underTest.commitMap.firstKey();
    int count = 0;
    while (startKey != null) {
      count++;
      startKey = underTest.commitMap.higherKey(KafkaRecordKey.maxKey(startKey.getTopicPartition()));
    }
    return count;
  }

  @Test
  void unregisterTest() {

    TestContainer aTopic1 = register("aTopic", 1);
    TestContainer aTopic2 = register("aTopic", 2);
    TestContainer aTopic3 = register("aTopic", 3);
    TestContainer bTopic2 = register("bTopic", 2);
    TestContainer cTopic2 = register("cTopic", 2);

    assertThat(partitionCount()).isEqualTo(5);
    assertThat(aTopic1.aborted).isFalse();
    assertThat(aTopic2.aborted).isFalse();
    assertThat(aTopic3.aborted).isFalse();
    assertThat(bTopic2.aborted).isFalse();
    assertThat(cTopic2.aborted).isFalse();
    assertThat(underTest.commitMap).hasSize(15);

    underTest.unregisterPartitions(List.of(new TopicPartition("aTopic", 2)));
    assertThat(partitionCount()).isEqualTo(4);
    assertThat(aTopic1.aborted).isFalse();
    assertThat(aTopic2.aborted).isTrue();
    assertThat(aTopic3.aborted).isFalse();
    assertThat(bTopic2.aborted).isFalse();
    assertThat(cTopic2.aborted).isFalse();
    assertThat(underTest.commitMap).hasSize(12);

    // test remvoe multiple includeing one with no records.
    underTest.unregisterPartitions(
        List.of(
            new TopicPartition("aTopic", 1),
            new TopicPartition("bTopic", 1),
            new TopicPartition("bTopic", 2)));
    assertThat(partitionCount()).isEqualTo(2);
    assertThat(aTopic1.aborted).isTrue();
    assertThat(aTopic2.aborted).isTrue();
    assertThat(aTopic3.aborted).isFalse();
    assertThat(bTopic2.aborted).isTrue();
    assertThat(cTopic2.aborted).isFalse();
    assertThat(underTest.commitMap).hasSize(6);
  }

  record TestEntry(String topic, int partition, long offset)
      implements Map.Entry<TopicPartition, OffsetAndMetadata> {

    @Override
    public TopicPartition getKey() {
      return new TopicPartition(topic, partition);
    }

    @Override
    public OffsetAndMetadata getValue() {
      return new OffsetAndMetadata(offset);
    }

    @Override
    public OffsetAndMetadata setValue(OffsetAndMetadata offsetAndMetadata) {
      throw new UnsupportedOperationException();
    }
  }

  class TestContainer implements StreamStateContainer {

    StreamState state;
    boolean aborted;

    TestContainer() {
      state = StreamState.CREATED;
    }

    @Override
    public boolean stateIs(StreamState state) {
      return this.state == state;
    }

    @Override
    public void abort() {
      aborted = true;
    }
  }
}
