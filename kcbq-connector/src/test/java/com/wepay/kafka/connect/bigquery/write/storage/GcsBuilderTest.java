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

package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GcsBuilderTest {

  @Test
  public void testStorageBuild() {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "abcd");
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dummy_dataset");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    Storage actualSettings = new GcpClientBuilder.GcsBuilder()
        .withConfig(config)
        .build();

    assertEquals(actualSettings.getOptions().getProjectId(), "abcd");
  }

}
