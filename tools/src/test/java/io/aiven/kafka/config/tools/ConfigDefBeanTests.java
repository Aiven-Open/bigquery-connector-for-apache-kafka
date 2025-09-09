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

package io.aiven.kafka.config.tools;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfigDefBeanTests {

    @Test
    public void testParents() {
        List<String> expected = Arrays.asList(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, BigQuerySinkConfig.DELETE_ENABLED_CONFIG, BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG);
        ConfigDefBean underTest = new ConfigDefBean();
        List<ConfigKeyBean> parents = underTest.parents();
        Collections.sort(expected);
        assertEquals(expected, parents.stream().map(ConfigKeyBean::getName).collect(Collectors.toList()));

        parents = underTest.parents(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG);

        assertEquals(2, parents.size());
        expected = Arrays.asList(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, BigQuerySinkConfig.DELETE_ENABLED_CONFIG);
        Collections.sort(expected);
        assertEquals(expected, parents.stream().map(ConfigKeyBean::getName).collect(Collectors.toList()));
    }

    @Test
    public void testDependents() {
        List<String> expected = Arrays.asList(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, BigQuerySinkConfig.INTERMEDIATE_TABLE_SUFFIX_CONFIG, BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG,
                BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG, BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
        ConfigDefBean underTest = new ConfigDefBean();
        List<ConfigKeyBean> deps = underTest.dependents();
        Collections.sort(expected);
        assertEquals(expected, deps.stream().map(ConfigKeyBean::getName).collect(Collectors.toList()));
    }

    @Test
    public void options() {
        ConfigDefBean underTest = new ConfigDefBean();
        List<ConfigKeyBean> opts =  underTest.configKeys();
        assertNotNull(opts);
    }

}
