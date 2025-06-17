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
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Storage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class GcpClientBuilderProjectTest {

  private static GoogleCredentials creds(String projectId, String quotaProject) throws NoSuchAlgorithmException {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(1024);
    KeyPair pair = gen.generateKeyPair();

    ServiceAccountCredentials.Builder builder = ServiceAccountCredentials.newBuilder()
        .setClientId("dummy-client")
        .setClientEmail("dummy@example.com")
        .setPrivateKey(pair.getPrivate())
        .setPrivateKeyId("dummy-key");

    if (projectId != null) {
      builder.setProjectId(projectId);
    }
    if (quotaProject != null) {
      builder.setQuotaProjectId(quotaProject);
    }

    return builder.build();
  }

  private static String getProject(GcpClientBuilder<?> builder) {
    try {
      java.lang.reflect.Field f = GcpClientBuilder.class.getDeclaredField("project");
      f.setAccessible(true);
      return (String) f.get(builder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getQuotaProject(GcpClientBuilder<?> builder) {
    try {
      java.lang.reflect.Field f = TestGcpClientBuilder.class.getDeclaredField("creds");
      f.setAccessible(true);
      GoogleCredentials creds = (GoogleCredentials) f.get(builder);
      return creds == null ? null : creds.getQuotaProjectId();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private abstract static class TestGcpClientBuilder<ClientT> extends GcpClientBuilder<ClientT> {
    private final GoogleCredentials creds;

    TestGcpClientBuilder(GoogleCredentials creds) {
      this.creds = creds;
    }

    @Override
    public TestGcpClientBuilder<ClientT> withConfig(BigQuerySinkConfig config) {
      super.withConfig(config);
      return this;
    }

    @Override
    public ClientT build() {
      if (useCredentialsProjectId) {
        return doBuild(getQuotaProject(this), creds);
      }
      return doBuild(getProject(this), creds);
    }
  }

  private static class TestBigQueryBuilder extends TestGcpClientBuilder<BigQuery> {
    TestBigQueryBuilder(GoogleCredentials creds) {
      super(creds);
    }

    @Override
    protected BigQuery doBuild(String project, GoogleCredentials credentials) {
      GcpClientBuilder.BigQueryBuilder delegate = new GcpClientBuilder.BigQueryBuilder();
      delegate.useCredentialsProjectId = this.useCredentialsProjectId;
      return delegate.doBuild(project, credentials);
    }
  }

  private static class TestGcsBuilder extends TestGcpClientBuilder<Storage> {
    TestGcsBuilder(GoogleCredentials creds) {
      super(creds);
    }

    @Override
    protected Storage doBuild(String project, GoogleCredentials credentials) {
      GcpClientBuilder.GcsBuilder delegate = new GcpClientBuilder.GcsBuilder();
      delegate.useCredentialsProjectId = this.useCredentialsProjectId;
      return delegate.doBuild(project, credentials);
    }
  }

  private static class TestWriteSettingsBuilder extends TestGcpClientBuilder<BigQueryWriteSettings> {
    TestWriteSettingsBuilder(GoogleCredentials creds) {
      super(creds);
    }

    @Override
    protected BigQueryWriteSettings doBuild(String project, GoogleCredentials credentials) {
      GcpClientBuilder.BigQueryWriteSettingsBuilder delegate = new GcpClientBuilder.BigQueryWriteSettingsBuilder();
      delegate.useCredentialsProjectId = this.useCredentialsProjectId;
      if (!this.useCredentialsProjectId) {
        return delegate.doBuild(project, credentials);
      }
      return delegate.doBuild(credentials.getQuotaProjectId(), credentials);
    }
  }

  private static BigQuerySinkConfig baseConfig(boolean flag) {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getString(BigQuerySinkConfig.PROJECT_CONFIG)).thenReturn("config_project");
    when(config.getString(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG)).thenReturn("dataset");
    when(config.getKeySource()).thenReturn(GcpClientBuilder.KeySource.JSON);
    when(config.getKey()).thenReturn("unused");
    when(config.getBoolean(BigQuerySinkConfig.USE_CREDENTIALS_PROJECT_ID_CONFIG)).thenReturn(flag);
    when(config.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
    return config;
  }

  @Test
  public void testProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    BigQuery result = new TestBigQueryBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("cred_project", result.getOptions().getProjectId());
  }

  @Test
  public void testConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    BigQuery result = new TestBigQueryBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getOptions().getProjectId());
  }

  @Test
  public void testStorageProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    Storage result = new TestGcsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("cred_project", result.getOptions().getProjectId());
  }

  @Test
  public void testStorageConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    Storage result = new TestGcsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getOptions().getProjectId());
  }

  @Test
  public void testWriteSettingsProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    BigQueryWriteSettings result = new TestWriteSettingsBuilder(creds)
        .withConfig(config)
        .build();

    assertNotEquals("config_project", result.getQuotaProjectId());
  }

  @Test
  public void testWriteSettingsConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project", "cred_quota_project");

    BigQueryWriteSettings result = new TestWriteSettingsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getQuotaProjectId());
  }
}