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

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.MERGE_RECORDS_THRESHOLD_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UpsertDeleteValidator extends MultiPropertyValidator<BigQuerySinkConfig> {
  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      MERGE_INTERVAL_MS_CONFIG, MERGE_RECORDS_THRESHOLD_CONFIG,
      KAFKA_KEY_FIELD_NAME_CONFIG,
      USE_STORAGE_WRITE_API_CONFIG
  ));
  private static final Logger logger = LoggerFactory.getLogger(UpsertDeleteValidator.class);

  private UpsertDeleteValidator(String propertyName) {
    super(propertyName);
  }

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!modeEnabled(config)) {
      return Optional.empty();
    }


    if (!config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)) {
      long mergeInterval = config.getLong(MERGE_INTERVAL_MS_CONFIG);
      long mergeRecordsThreshold = config.getLong(MERGE_RECORDS_THRESHOLD_CONFIG);
      if (mergeInterval == -1 && mergeRecordsThreshold == -1) {
        return Optional.of(String.format(
            "%s and %s cannot both be -1",
            MERGE_INTERVAL_MS_CONFIG,
            MERGE_RECORDS_THRESHOLD_CONFIG
        ));
      }

      if (mergeInterval != -1 && mergeInterval < 10_000L) {
        logger.warn(
            "{} should not be set to less than 10 seconds. "
                + "A validation would be introduced in a future release to this effect.",
            MERGE_INTERVAL_MS_CONFIG
        );
      }

      if (!config.getKafkaKeyFieldName().isPresent()) {
        return Optional.of(String.format(
            "%s must be specified when %s is set to true",
            KAFKA_KEY_FIELD_NAME_CONFIG,
            propertyName()
        ));
      }
    } else {
      for (String property : Arrays.asList(MERGE_INTERVAL_MS_CONFIG, MERGE_RECORDS_THRESHOLD_CONFIG)) {
        if (config.originals().containsKey(MERGE_INTERVAL_MS_CONFIG)) {
          logger.warn("The {} property will be ignored because {} is set to true",
              property,
              USE_STORAGE_WRITE_API_CONFIG
          );
        }
      }

      if (!config.isUpsertEnabled() && config.isDeleteEnabled()) {
        return Optional.of(String.format(
            "Delete-only mode is not supported when the Storage Write API is enabled "
              + "(%s = true); please either disable delete support (set %s to false) "
              + "or enable upsert support (set %s to false)",
            USE_STORAGE_WRITE_API_CONFIG,
            DELETE_ENABLED_CONFIG,
            UPSERT_ENABLED_CONFIG
        ));
      }

      Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
      if (!kafkaKeyFieldName.isPresent()) {
        logger.warn(
            "Defaulting to a value of '' for the {} property because both "
                + "upsert/delete support and the Storage Write API are both enabled",
            KAFKA_KEY_FIELD_NAME_CONFIG
        );
      } else if (!"".equals(kafkaKeyFieldName.get())) {
        return Optional.of(String.format(
            "The only accepted value for the %s property is '' when "
                + "upsert/delete support and the Storage Write API are both enabled",
            KAFKA_KEY_FIELD_NAME_CONFIG
        ));
      }
    }

    return Optional.empty();
  }

  /**
   * @param config the user-provided configuration
   * @return whether the write mode for the validator (i.e., either upsert or delete) is enabled
   */
  protected abstract boolean modeEnabled(BigQuerySinkConfig config);

  public static class UpsertValidator extends UpsertDeleteValidator {
    public UpsertValidator() {
      super(UPSERT_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(UPSERT_ENABLED_CONFIG);
    }
  }

  public static class DeleteValidator extends UpsertDeleteValidator {
    public DeleteValidator() {
      super(DELETE_ENABLED_CONFIG);
    }

    @Override
    protected boolean modeEnabled(BigQuerySinkConfig config) {
      return config.getBoolean(DELETE_ENABLED_CONFIG);
    }
  }
}
