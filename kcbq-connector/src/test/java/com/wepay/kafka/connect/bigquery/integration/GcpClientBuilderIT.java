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

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
public class GcpClientBuilderIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(GcpClientBuilderIT.class);

  private TableId tableId;

  @BeforeEach
  public void setup() throws Exception {
    BigQuery bigQuery = newBigQuery();
    tableId = TableId.of(project(), dataset(), "authenticate-storage-api");
    if (bigQuery.getTable(tableId) == null) {
      logger.info("Going to Create table : " + tableId.toString());
      bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.newBuilder().build()));
      logger.info("Created table : " + tableId.toString());
      // table takes time after creation before being available for operations. You may have to wait a few minutes (~5 minutes)
      // Try to wait for 5 minutes if table is seen.
      int attempts = 10;
      while (bigQuery.getTable(tableId) == null && attempts > 0) {
        logger.debug("Busy waiting for table {} to appear! Attempt {}", tableId.getTable(), (10 - attempts));
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        attempts--;
      }
      if (attempts == 0) {
        throw new AssertionError(
            "Created table is not yet available. Re-run test after a few minutes");
      }
    }
  }

  private Map<String, String> connectorProps(GcpClientBuilder.KeySource keySource) throws IOException {
    Map<String, String> properties = baseConnectorProps(1);
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, keySource.name());

    if (keySource == GcpClientBuilder.KeySource.APPLICATION_DEFAULT) {
      properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, null);
    } else if (keySource == GcpClientBuilder.KeySource.JSON) {
      // actually keyFile is the path to the credentials file, so we convert it to the json string
      String credentialsJsonString = new String(Files.readAllBytes(Paths.get(keyFile())), StandardCharsets.UTF_8);
      properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, credentialsJsonString);
    }

    return properties;
  }

  /**
   * Construct the BigQuery and Storage clients and perform some basic operations to check they are operational.
   *
   * @param keySource the key Source to use
   * @throws IOException
   */
  private void testClients(GcpClientBuilder.KeySource keySource) throws Exception {
    Map<String, String> properties = connectorProps(keySource);
    properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    BigQuery bigQuery = new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
    bigQuery.listTables(DatasetId.of(dataset()));

    try (Storage storage = new GcpClientBuilder.GcsBuilder().withConfig(config).build()) {
      storage.get(gcsBucket());
    }

    BigQueryWriteSettings settings = new GcpClientBuilder.BigQueryWriteSettingsBuilder().withConfig(config).build();
    BigQueryWriteClient client = BigQueryWriteClient.create(settings);
    JsonStreamWriter.Builder writerBuilder = JsonStreamWriter.newBuilder(
        TableNameUtils.tableName(tableId).toString(),
        client
    );
    try (JsonStreamWriter writer = writerBuilder.build()) {
      assertTrue(writer.getStreamName().contains("default"));
    }
  }

  @Test
  public void testApplicationDefaultCredentials() throws Exception {
    testClients(GcpClientBuilder.KeySource.APPLICATION_DEFAULT);
  }

  @Test
  public void testFile() throws Exception {
    testClients(GcpClientBuilder.KeySource.FILE);
  }

  @Test
  public void testJson() throws Exception {
    testClients(GcpClientBuilder.KeySource.JSON);
  }

}
