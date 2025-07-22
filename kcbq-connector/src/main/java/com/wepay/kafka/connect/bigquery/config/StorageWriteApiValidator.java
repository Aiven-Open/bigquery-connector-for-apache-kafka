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

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.DELETE_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.UPSERT_ENABLED_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class StorageWriteApiValidator extends MultiPropertyValidator<BigQuerySinkConfig> {

  public static final String upsertNotSupportedError = "Upsert mode is not supported with Storage Write API."
      + " Either disable Upsert mode or disable Storage Write API";
  public static final String legacyBatchNotSupportedError = "Legacy Batch mode is not supported with Storage Write API."
      + " Either disable Legacy Batch mode or disable Storage Write API";
  public static final String newBatchNotSupportedError = "Storage Write Api Batch load is supported only when useStorageWriteApi is "
      + "enabled. Either disable batch mode or enable Storage Write API";
  public static final String deleteNotSupportedError = "Delete mode is not supported with Storage Write API. Either disable Delete mode "
      + "or disable Storage Write API";
  public static final String partitionDecoratorNewBatchNotSupported = "Partition decorator syntax is not supported with Storage Write API Batch Load. "
                  + "It is currently only available when using the Storage Write API default stream.";
  private static final Collection<String> DEPENDENTS = Collections.unmodifiableCollection(Arrays.asList(
      UPSERT_ENABLED_CONFIG, DELETE_ENABLED_CONFIG, ENABLE_BATCH_CONFIG
  ));

  protected StorageWriteApiValidator(String propertyName) {
    super(propertyName);
  }

  protected StorageWriteApiValidator() {
    super(USE_STORAGE_WRITE_API_CONFIG);
  }

  @Override
  protected Collection<String> dependents() {
    return DEPENDENTS;
  }

  @Override
  protected Optional<String> doValidate(BigQuerySinkConfig config) {
    if (!config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)) {
      if (config.getBoolean(ENABLE_BATCH_MODE_CONFIG)) {
        return Optional.of(newBatchNotSupportedError);
      }
      //No legacy modes validation needed if not using new api
      return Optional.empty();
    }
    if (config.getBoolean(UPSERT_ENABLED_CONFIG)) {
      return Optional.of(upsertNotSupportedError);
    } else if (config.getBoolean(DELETE_ENABLED_CONFIG)) {
      return Optional.of(deleteNotSupportedError);
    } else if (!config.getList(ENABLE_BATCH_CONFIG).isEmpty()) {
      return Optional.of(legacyBatchNotSupportedError);
    } else if (config.originals().containsKey(BIGQUERY_PARTITION_DECORATOR_CONFIG)
        && config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG) && config.getBoolean(ENABLE_BATCH_MODE_CONFIG)
    ) {
      // Only report an error if the user explicitly requested partition decorator syntax and batch load;
      // if they didn't, then we can silently disable it when using the Storage Write API Batch Load
      return Optional.of(partitionDecoratorNewBatchNotSupported);
    }

    return Optional.empty();
  }

  static class StorageWriteApiBatchValidator extends StorageWriteApiValidator {
    StorageWriteApiBatchValidator() {
      super(ENABLE_BATCH_MODE_CONFIG);
    }
  }
}
