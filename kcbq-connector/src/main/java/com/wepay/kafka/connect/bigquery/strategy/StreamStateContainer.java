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

import com.wepay.kafka.connect.bigquery.write.storage.StreamState;

/**
 * A container for the stream state. This container will be associated with one or more rows. Once
 * the container state reaches {@link StreamState#COMMITTED} or {@link StreamState#INACTIVE} Kafka
 * will be allowed to commit the associated topic-partition-offset. In some CommitStrategies the
 * state of the container is not checked, for example, {@link DefaultKafkaCommitStrategy}.
 */
public interface StreamStateContainer {
  /**
   * checks if the current state is the specified state.
   *
   * @param state the specified state.
   * @return {@code true} if the current state is the specified state.
   */
  boolean stateIs(StreamState state);

  /** Abort the stream writing and do not commit. */
  void abort();
}
