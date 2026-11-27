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
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** A commit strategy that does not use connect managed offset commits. */
public class NoCommitStrategy implements CommitStrategy {

  public NoCommitStrategy() {}

  @Override
  public <T extends StreamStateContainer> T queueKeys(
      Stream<KafkaRecordKey> keys, T streamStateContainer) {
    return streamStateContainer;
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return Collections.emptyMap();
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
