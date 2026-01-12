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
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_CREDENTIALS_PROJECT_ID_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.aiven.commons.google.auth.GCPValidator;
import io.aiven.commons.system.VersionInfo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
  private static final String USER_AGENT_HEADER_KEY = "user-agent";
  private static final String USER_AGENT_HEADER_FORMAT = "Google BigQuery Sink/%s (GPN: %s;)";

  protected HeaderProvider headerProvider = null;
  private String project = null;
  private KeySource keySource = null;
  private String key = null;

  private boolean useStorageWriteApi = false;
  protected boolean useCredentialsProjectId = false;

  public GcpClientBuilder<ClientT> withConfig(BigQuerySinkConfig config) {
    return withProject(config.getString(PROJECT_CONFIG))
    .withKeySource(config.getKeySource())
    .withKey(config.getKey())
    .withWriterApi(config.getBoolean(USE_STORAGE_WRITE_API_CONFIG))
    .withProjectFromCreds(config.getBoolean(USE_CREDENTIALS_PROJECT_ID_CONFIG))
    .withUserAgent();
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

  public GcpClientBuilder<ClientT> withProjectFromCreds(Boolean useCredentialsProjectId) {
    this.useCredentialsProjectId = useCredentialsProjectId;
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

  public GcpClientBuilder<ClientT> withUserAgent() {
    VersionInfo versionInfo = new VersionInfo(GcpClientBuilder.class);
    this.headerProvider = FixedHeaderProvider.create(
            USER_AGENT_HEADER_KEY,
            String.format(USER_AGENT_HEADER_FORMAT, versionInfo.getVersion(), versionInfo.getVendor())
    );
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
    if (!useCredentialsProjectId) {
      Objects.requireNonNull(project, "Project must be defined to build a GCP client");
    }    

    byte[] credentialsBytes;
    switch (keySource) {
      case JSON:
        credentialsBytes = key.getBytes(StandardCharsets.UTF_8);
        break;
      case FILE:
        try {
          logger.debug("Attempting to open file {} for service account json key", key);
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
          ByteStreams.copy(new FileInputStream(key), outputStream);
          credentialsBytes = outputStream.toByteArray();
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to access JSON key file", e);
        }
        break;
      case APPLICATION_DEFAULT:
        try {
          logger.debug("Attempting to use application default credentials");
          return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new BigQueryConnectException("Failed to create Application Default Credentials: " + e.getMessage(), e);
        }
      default:
        throw new IllegalArgumentException("Unexpected value for KeySource enum: " + keySource);
    }

    try {
      GCPValidator.validateCredentialJson(credentialsBytes);
      InputStream credentialsStream = new ByteArrayInputStream(credentialsBytes);
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
      BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
      if (headerProvider != null) {
        builder.setHeaderProvider(headerProvider);
      }
      if (!useCredentialsProjectId) {
        builder = builder.setProjectId(project);
      }

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
      StorageOptions.Builder builder = StorageOptions.newBuilder();
      if (headerProvider != null) {
        builder.setHeaderProvider(headerProvider);
      }
      if (!useCredentialsProjectId) {
        builder = builder.setProjectId(project);
      }

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
      BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder();
      if (headerProvider != null) {
        builder.setHeaderProvider(headerProvider);
      }
      if (!useCredentialsProjectId) {
        builder.setQuotaProjectId(project);
      }

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
