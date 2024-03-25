/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.utils;

/**
 * Largely adapted from the
 * <a href="https://github.com/apache/kafka/blob/011d2382689df7b850b05281f4e19387c42c1f4d/clients/src/main/java/org/apache/kafka/common/utils/Time.java">Kafka Time interface</a>,
 * which is not public API and therefore cannot be relied upon as a dependency.
 */
public interface Time {

  Time SYSTEM = new Time() {
    @Override
    public void sleep(long durationMs) throws InterruptedException {
      Thread.sleep(durationMs);
    }

    @Override
    public long milliseconds() {
      return System.currentTimeMillis();
    }
  };

  void sleep(long durationMs) throws InterruptedException;

  long milliseconds();

}
