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

package com.wepay.kafka.connect.bigquery.write;

import java.util.List;

public class RecordBatches<E> {

  private final List<E> records;

  private int batchStart;
  private int batchSize;

  public RecordBatches(List<E> records) {
    this.records = records;
    this.batchStart = 0;
    this.batchSize = records.size();
  }

  public List<E> currentBatch() {
    int size = Math.min(records.size() - batchStart, batchSize);
    return records.subList(batchStart, batchStart + size);
  }

  public void advanceToNextBatch() {
    batchStart += batchSize;
  }

  public void reduceBatchSize() {
    if (batchSize <= 1) {
      throw new IllegalStateException("Cannot reduce batch size any further");
    }
    batchSize /= 2;
  }

  public boolean completed() {
    return batchStart >= records.size();
  }

}
