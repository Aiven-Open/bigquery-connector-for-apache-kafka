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

/**
 * Contains the classes to track topic/partition/offset to rows in tables and ensure that the
 * topic/partitions are not committed too early.
 *
 * <p>The KafkaRecordKey tracks a single Topic/Partition/Offset.
 *
 * <p>The CommitStrategy contains a map of KafkaRecordKeys to StreamStateContainers. One key to one
 * container. A container may be pointed to by multiple Keys. CommitStrategy is responsible for
 * determining which topci/partition/offset may be committed when Kafka calls preCommit(). The
 * commit strategy also has a method that is called by the timer thread to allows cleanup of the
 * internal state if necessary.
 *
 * <p>StreamStateContainer is the class that tracks the status of a group of KafkaRecordKeys. In
 * general, Once the container reached the COMMITTED state the CommitStrategy will report the
 * highest offsets as commitable to Kafka. The container also implements an abort() method that will
 * stop the container from writing the data to the storage, this is used when Kafka unassignes a
 * partition.
 *
 * <p>Suggestions:
 *
 * <p>Different partitions should either be queued for write in different StreamStateContainers or
 * the container should be able to remove topic/partitions from the queued data.
 */
