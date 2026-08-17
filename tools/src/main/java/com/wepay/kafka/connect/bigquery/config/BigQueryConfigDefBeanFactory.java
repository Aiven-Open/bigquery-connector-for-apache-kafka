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

package com.wepay.kafka.connect.bigquery.config;

import io.aiven.commons.kafka.config.SinceInfoMapBuilder;
import io.aiven.commons.kafka.config.docs.ConfigDefBean;
import io.aiven.commons.kafka.config.docs.ExtendedConfigKeyBean;
import java.io.IOException;
import java.io.InputStream;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.velocity.tools.config.DefaultKey;
import org.apache.velocity.tools.config.ValidScope;
import org.slf4j.LoggerFactory;

/**
 * A BaseConfigDefBean instance that uses the {@link BigQuerySinkConfig#getConfig} for data and
 * returns {@link ExtendedConfigKeyBean} objects.
 */
@SuppressWarnings("unused")
@DefaultKey("BQConfigDefFactory")
@ValidScope({"application"})
public class BigQueryConfigDefBeanFactory {
  /** Constructor. */
  public BigQueryConfigDefBeanFactory() {}

  public ConfigDefBean<ExtendedConfigKeyBean> open() {
    final ConfigDef configDef = BigQuerySinkConfig.getConfig();

    final String versionMap = BigQuerySinkConfig.class.getName().replace(".", "/") + ".versionMap";
    InputStream inputStream =
        BigQuerySinkConfig.class.getClassLoader().getResourceAsStream(versionMap);
    if (inputStream != null) {
      try {
        SinceInfoMapBuilder builder = new SinceInfoMapBuilder();
        builder.parse(inputStream);
        builder.applyTo(configDef);
      } catch (IOException e) {
        LoggerFactory.getLogger(ConfigDefBean.class)
            .error("Unable to appy {}: {}", versionMap, e.getMessage(), e);
      } finally {
        try {
          inputStream.close();
        } catch (IOException e) {
          LoggerFactory.getLogger(ConfigDefBean.class).error("Error closing input stream", e);
        }
      }
    }
    return new ConfigDefBean<>(BigQuerySinkConfig.getConfig(), ExtendedConfigKeyBean::new);
  }
}
