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
import static com.wepay.kafka.connect.bigquery.utils.GsonUtils.getAsString;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.AwsCredentials;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import io.aiven.commons.util.google.auth.GCPValidator;
import io.aiven.commons.util.system.VersionInfo;
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
  private static final Collection<String> scopes =
      Lists.newArrayList(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/bigquery.insertdata",
          "https://www.googleapis.com/auth/cloud-platform",
          "https://www.googleapis.com/auth/cloud-platform.read-only",
          "https://www.googleapis.com/auth/devstorage.full_control",
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/devstorage.read_write");
  private static final String USER_AGENT_HEADER_KEY = "user-agent";
  private static final String USER_AGENT_HEADER_FORMAT = "Google BigQuery Sink/%s (GPN: %s;)";
  // subject_token_type identifying an AWS external_account: the subject token is a SigV4-signed
  // GetCallerIdentity request rather than a fetched JWT. Sourced from google-auth's own enum so it
  // stays in sync with the library (value: urn:ietf:params:aws:token-type:aws4_request).
  private static final String AWS_SUBJECT_TOKEN_TYPE =
      ExternalAccountCredentials.SubjectTokenTypes.AWS4.value;
  private static final Gson GSON = new Gson();

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
    this.headerProvider =
        FixedHeaderProvider.create(
            USER_AGENT_HEADER_KEY,
            String.format(
                USER_AGENT_HEADER_FORMAT, versionInfo.getVersion(), versionInfo.getVendor()));
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
      case WIF_JSON:
        logger.debug("Attempting to use Workload Identity Federation (WIF_JSON) credentials");
        return wifCredentials(key);
      case APPLICATION_DEFAULT:
        try {
          logger.debug("Attempting to use application default credentials");
          return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new BigQueryConnectException(
              "Failed to create Application Default Credentials: " + e.getMessage(), e);
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

  /**
   * Builds credentials for {@code keySource=WIF_JSON}: an {@code external_account} configuration
   * that authenticates via Workload Identity Federation. Dispatches on {@code subject_token_type}.
   * Only AWS is supported today; other providers (OIDC/Azure) present a plain JWT that google-auth
   * fetches natively via {@code fromStream} and would get their own branch here when implemented.
   */
  private GoogleCredentials wifCredentials(String keyJson) {
    JsonObject config;
    try {
      config = GSON.fromJson(keyJson, JsonObject.class);
    } catch (JsonSyntaxException e) {
      throw new BigQueryConnectException("WIF_JSON keyfile is not valid JSON", e);
    }
    if (config == null) {
      throw new BigQueryConnectException("WIF_JSON keyfile is empty or not valid JSON");
    }

    String subjectTokenType = getAsString(config, "subject_token_type");
    if (AWS_SUBJECT_TOKEN_TYPE.equals(subjectTokenType)) {
      return awsWifCredentials(config);
    }

    // Non-AWS providers (OIDC/Azure) present a plain JWT that google-auth fetches natively via
    // fromStream (using credential_source), so they need no custom supplier. That path is not
    // implemented or tested here; a future implementation adds its own branch that retains
    // credential_source. Fail fast rather than guess the provider.
    throw new BigQueryConnectException(
        "WIF_JSON currently supports only AWS external_account credentials (subject_token_type="
            + AWS_SUBJECT_TOKEN_TYPE
            + "); got: "
            + subjectTokenType);
  }

  /**
   * Builds AWS {@link AwsCredentials} from a WIF {@code external_account} config. The AWS-specific
   * handling is isolated here: {@code credential_source} is stripped so the keyfile passes {@link
   * GCPValidator}'s URI allowlist, and AWS credentials are provided by {@link
   * EcsAwsSecurityCredentialsSupplier} rather than google-auth's built-in AWS provider, which
   * cannot read the ECS/Fargate container credentials endpoint.
   */
  private GoogleCredentials awsWifCredentials(JsonObject config) {
    // credential_source carries the regional STS verification URL, which we read before removing
    // it.
    // GCPValidator rejects the AWS credential_source URIs by default and the supplier provides AWS
    // credentials directly, so credential_source is stripped before validation and is never used.
    String regionalUrl = null;
    if (config.has("credential_source") && config.get("credential_source").isJsonObject()) {
      regionalUrl =
          getAsString(
              config.getAsJsonObject("credential_source"), "regional_cred_verification_url");
    }
    config.remove("credential_source");

    byte[] validatedBytes = GSON.toJson(config).getBytes(StandardCharsets.UTF_8);
    try {
      GCPValidator.validateCredentialJson(validatedBytes);
    } catch (IOException e) {
      throw new BigQueryConnectException("Failed to validate WIF_JSON credentials", e);
    }

    String audience = getAsString(config, "audience");
    String tokenUrl = getAsString(config, "token_url");
    if (audience == null || tokenUrl == null) {
      throw new BigQueryConnectException("WIF_JSON AWS keyfile must define audience and token_url");
    }

    logger.debug("Building AWS WIF credentials with ECS/Fargate container credentials supplier");
    AwsCredentials.Builder builder =
        AwsCredentials.newBuilder()
            .setAudience(audience)
            .setSubjectTokenType(AWS_SUBJECT_TOKEN_TYPE)
            .setTokenUrl(tokenUrl)
            .setAwsSecurityCredentialsSupplier(new EcsAwsSecurityCredentialsSupplier());

    String impersonationUrl = getAsString(config, "service_account_impersonation_url");
    if (impersonationUrl != null) {
      builder.setServiceAccountImpersonationUrl(impersonationUrl);
    }
    if (regionalUrl != null) {
      builder.setRegionalCredentialVerificationUrlOverride(regionalUrl);
    }
    if (useStorageWriteApi) {
      builder.setScopes(scopes);
    }
    return builder.build();
  }

  protected abstract ClientT doBuild(String project, GoogleCredentials credentials);

  public enum KeySource {
    FILE,
    JSON,
    APPLICATION_DEFAULT,
    WIF_JSON
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
   * Prepares BigQuery Write settings object which includes project info, header info, credentials
   * etc.
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
        throw new BigQueryStorageWriteApiConnectException(
            "Failed to create Storage API write settings", e);
      }
    }
  }
}
