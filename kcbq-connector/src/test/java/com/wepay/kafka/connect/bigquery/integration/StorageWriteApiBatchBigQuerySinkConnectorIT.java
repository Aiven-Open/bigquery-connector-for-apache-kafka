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

package com.wepay.kafka.connect.bigquery.integration;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import java.util.Map;
import org.junit.jupiter.api.Tag;

@Tag("integration")
public class StorageWriteApiBatchBigQuerySinkConnectorIT extends StorageWriteApiBigQuerySinkConnectorIT {

  @Override
  protected Map<String, String> configs(String topic) {
    Map<String, String> result = super.configs(topic);
    result.put(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG, "true");
    result.put(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG, "15");
    return result;
  }

  @Override
  protected String topic(String basename) {
    return super.topic(basename + "-batch-mode");
  }

  @Override
  protected boolean isBatchMode() {
    return true;
  }}
