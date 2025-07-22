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
import static com.wepay.kafka.connect.bigquery.config.StorageWriteApiValidator.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class StorageWriteApiValidatorTest {

  @Test
  public void testNoStorageWriteApiEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);

    assertEquals(Optional.empty(), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testNoLegacyModesEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
    when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

    assertEquals(Optional.empty(), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testUpsertModeEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(true);
    when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

    assertEquals(
        Optional.of(upsertNotSupportedError),
        new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testDeleteModeEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
    when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(true);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.emptyList());

    assertEquals(Optional.of(deleteNotSupportedError), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testLegacyBatchModeEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(UPSERT_ENABLED_CONFIG)).thenReturn(false);
    when(config.getBoolean(DELETE_ENABLED_CONFIG)).thenReturn(false);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

    assertEquals(Optional.of(legacyBatchNotSupportedError), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testPartitionDecoratorAndBatchLoadExplicitlyEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    // User explicitly requested partition decorator syntax
    when(config.originals()).thenReturn(Collections.singletonMap(BIGQUERY_PARTITION_DECORATOR_CONFIG, "true"));

    assertEquals(Optional.of(partitionDecoratorNewBatchNotSupported), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testBatchLoadEnabledAndPartitionDecoratorImplicitlyEnabled() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
    // User didn't explicitly ask for partition decorator syntax
    when(config.originals()).thenReturn(Collections.emptyMap());

    assertEquals(Optional.empty(), new StorageWriteApiValidator().doValidate(config));
  }

  @Test
  public void testNewBatchModeEnabledWithoutNewApi() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
    when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);

    assertEquals(Optional.of(newBatchNotSupportedError),
        new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
  }

  @Test
  public void testNewBatchModeEnabledWithNewApi() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);

    assertEquals(Optional.empty(),
        new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
  }

  @Test
  public void testBothLegacyAndNewBatchEnabledOldApi() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
    when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

    assertEquals(Optional.of(newBatchNotSupportedError),
        new StorageWriteApiValidator.StorageWriteApiBatchValidator().doValidate(config));
  }

  @Test
  public void testBothLegacyAndNewBatchEnabledNewApi() {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);

    when(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);
    when(config.getBoolean(ENABLE_BATCH_MODE_CONFIG)).thenReturn(true);
    when(config.getList(ENABLE_BATCH_CONFIG)).thenReturn(Collections.singletonList("abc"));

    assertEquals(Optional.of(legacyBatchNotSupportedError), new StorageWriteApiValidator().doValidate(config));
  }
}

