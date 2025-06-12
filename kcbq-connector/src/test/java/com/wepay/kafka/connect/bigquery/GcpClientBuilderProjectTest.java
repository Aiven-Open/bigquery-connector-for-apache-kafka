package com.wepay.kafka.connect.bigquery;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Storage;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GcpClientBuilderProjectTest {

  private static GoogleCredentials creds(String quotaProject) {
    GoogleCredentials base = GoogleCredentials.create(new AccessToken("token", null));
    if (quotaProject != null) {
      base = base.createWithQuotaProject(quotaProject);
    }
    return base;
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

  private abstract static class TestGcpClientBuilder<ClientT> extends GcpClientBuilder<ClientT> { // IS THAT REQUIRED???
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
      if (useProjectFromCreds) {
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
      delegate.useProjectFromCreds = this.useProjectFromCreds; // THIS SHOULD BE ENOUGH?
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
      delegate.useProjectFromCreds = this.useProjectFromCreds;
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
      delegate.useProjectFromCreds = this.useProjectFromCreds;
      return delegate.doBuild(project, credentials);
    }
  }

  private static BigQuerySinkConfig baseConfig(boolean flag) {
    BigQuerySinkConfig config = mock(BigQuerySinkConfig.class);
    when(config.getString(BigQuerySinkConfig.PROJECT_CONFIG)).thenReturn("config_project");
    when(config.getString(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG)).thenReturn("dataset");
    when(config.getKeySource()).thenReturn(GcpClientBuilder.KeySource.FILE);
    when(config.getKey()).thenReturn("unused");
    when(config.getBoolean(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG)).thenReturn(flag);
    when(config.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
    return config;
  }

  @Test
  public void testProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project");

    BigQuery result = new TestBigQueryBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("cred_project", result.getOptions().getQuotaProjectId());
  }

  @Test
  public void testConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project");

    BigQuery result = new TestBigQueryBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getOptions().getProjectId());
  }

  @Test
  public void testStorageProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project");

    Storage result = new TestGcsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("cred_project", result.getOptions().getQuotaProjectId());
  }

  @Test
  public void testStorageConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project");

    Storage result = new TestGcsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getOptions().getProjectId());
  }

  @Test
  public void testWriteSettingsProjectFromCredentialsFlagTrue() throws Exception {
    BigQuerySinkConfig config = baseConfig(true);
    GoogleCredentials creds = creds("cred_project");

    BigQueryWriteSettings result = new TestWriteSettingsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("cred_project", result.getQuotaProjectId());
  }

  @Test
  public void testWriteSettingsConfigProjectWhenFlagFalse() throws Exception {
    BigQuerySinkConfig config = baseConfig(false);
    GoogleCredentials creds = creds("cred_project");

    BigQueryWriteSettings result = new TestWriteSettingsBuilder(creds)
        .withConfig(config)
        .build();

    assertEquals("config_project", result.getQuotaProjectId());
  }
}
