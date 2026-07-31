/*
 * Copyright 2026 Aiven Oy and
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
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.config;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class KafkaKeyFieldNameValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  public KafkaKeyFieldNameValidator() {
    super(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
  }

  @Override
  protected Collection<String> dependents() {
    return List.of();
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    //  Kafka key field name should be null if storage write API is used with upsert or delete
    // enabled.
    final String value = config.getString(BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
    final boolean upsertOrDelete = config.isUpsertEnabled() || config.isDeleteEnabled();
    if (config.useStorageWriteApi()) {
      if (StringUtils.isNotBlank(value) && upsertOrDelete) {
        return Optional.of(
            String.format(
                "%s may not be set if %s is set and either %s or %s are set.",
                BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG,
                BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG,
                BigQuerySinkConfig.UPSERT_ENABLED_CONFIG,
                BigQuerySinkConfig.DELETE_ENABLED_CONFIG));
      }
    } else {
      if (StringUtils.isBlank(value) && upsertOrDelete) {
        return Optional.of(
            String.format(
                "%s must be specified when %s or %s is set to true and %s is false",
                KAFKA_KEY_FIELD_NAME_CONFIG,
                BigQuerySinkConfig.UPSERT_ENABLED_CONFIG,
                BigQuerySinkConfig.DELETE_ENABLED_CONFIG,
                BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG));
      }
    }
    return Optional.empty();
  }
}
