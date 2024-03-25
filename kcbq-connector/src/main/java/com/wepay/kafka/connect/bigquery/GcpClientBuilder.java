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

package com.wepay.kafka.connect.bigquery;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.PROJECT_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GcpClientBuilder<ClientT> {

  private static final Logger logger = LoggerFactory.getLogger(GcpClientBuilder.class);
  // Scope list taken from : https://developers.google.com/identity/protocols/oauth2/scopes#bigquery
  private static final Collection<String> scopes = Lists.newArrayList(
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/bigquery.insertdata",
      "https://www.googleapis.com/auth/cloud-platform",
      "https://www.googleapis.com/auth/cloud-platform.read-only",
      "https://www.googleapis.com/auth/devstorage.full_control",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/devstorage.read_write"
  );
  private String project = null;
  private KeySource keySource = null;
  private String key = null;

  private boolean useStorageWriteApi = false;

  public GcpClientBuilder<ClientT> withConfig(BigQuerySinkConfig config) {
    return withProject(config.getString(PROJECT_CONFIG))
        .withKeySource(config.getKeySource())
        .withKey(config.getKey())
        .withWriterApi(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG));
  }

  public GcpClientBuilder<ClientT> withProject(String project) {
    Objects.requireNonNull(project, "Project cannot be null");
    this.project = project;
    return this;
  }

  public GcpClientBuilder<ClientT> withWriterApi(Boolean useStorageWriteApi) {
    this.useStorageWriteApi = useStorageWriteApi;
    return this;
  }

  public GcpClientBuilder<ClientT> withKeySource(KeySource keySource) {
    Objects.requireNonNull(keySource, "Key cannot be null");
    this.keySource = keySource;
    return this;
  }

  public GcpClientBuilder<ClientT> withKey(String key) {
    this.key = key;
    return this;
  }

  public ClientT build() {
    return doBuild(project, credentials());
  }

  private GoogleCredentials credentials() {
    if (key == null && keySource != KeySource.APPLICATION_DEFAULT) {
      return null;
    }

    Objects.requireNonNull(keySource, "Key source must be defined to build a GCP client");
    Objects.requireNonNull(project, "Project must be defined to build a GCP client");

    InputStream credentialsStream;
    switch (keySource) {
      case JSON:
        credentialsStream = new ByteArrayInputStream(key.getBytes(StandardCharsets.UTF_8));
        break;
      case FILE:
        try {
          logger.debug("Attempting to open file {} for service account json key", key);
          credentialsStream = new FileInputStream(key);
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to access JSON key file", e);
        }
        break;
      case APPLICATION_DEFAULT:
        try {
          logger.debug("Attempting to use application default credentials");
          return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to create Application Default Credentials", e);
        }
      default:
        throw new IllegalArgumentException("Unexpected value for KeySource enum: " + keySource);
    }

    try {
      return useStorageWriteApi
          ? GoogleCredentials.fromStream(credentialsStream).createScoped(scopes)
          : GoogleCredentials.fromStream(credentialsStream);
    } catch (IOException e) {
      throw new BigQueryConnectException("Failed to create credentials from input stream", e);
    }
  }

  protected abstract ClientT doBuild(String project, GoogleCredentials credentials);

  public enum KeySource {
    FILE, JSON, APPLICATION_DEFAULT
  }

  public static class BigQueryBuilder extends GcpClientBuilder<BigQuery> {
    @Override
    protected BigQuery doBuild(String project, GoogleCredentials credentials) {
      BigQueryOptions.Builder builder = BigQueryOptions.newBuilder()
          .setProjectId(project);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access BigQuery without authentication");
      }

      return builder.build().getService();
    }
  }

  public static class GcsBuilder extends GcpClientBuilder<Storage> {
    @Override
    protected Storage doBuild(String project, GoogleCredentials credentials) {
      StorageOptions.Builder builder = StorageOptions.newBuilder()
          .setProjectId(project);

      if (credentials != null) {
        builder.setCredentials(credentials);
      } else {
        logger.debug("Attempting to access GCS without authentication");
      }

      return builder.build().getService();
    }
  }

  /**
   * Prepares BigQuery Write settings object which includes project info, header info, credentials etc.
   */
  public static class BigQueryWriteSettingsBuilder extends GcpClientBuilder<BigQueryWriteSettings> {

    @Override
    protected BigQueryWriteSettings doBuild(String project, GoogleCredentials credentials) {
      BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder()
          .setQuotaProjectId(project);

      if (credentials != null) {
        builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
      } else {
        logger.warn("Attempting to access GCS without authentication");
      }

      try {
        return builder.build();
      } catch (IOException e) {
        logger.error("Failed to create Storage API write settings due to {}", e.getMessage());
        throw new BigQueryStorageWriteApiConnectException("Failed to create Storage API write settings", e);
      }
    }
  }

}
