package com.wepay.kafka.connect.bigquery;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GcpClientBuilderProjectTest {

  private static String resourcePath(String name) {
    return GcpClientBuilderProjectTest.class.getClassLoader()
        .getResource("credentials/" + name).getPath();
  }

  @Test
  public void testProjectFromCredentialsFlagTrue() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryBuilder builder = new GcpClientBuilder.BigQueryBuilder();
    builder.withConfig(config);

    assertEquals("cred_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testConfigProjectWhenCredsMissing() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account_no_project.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryBuilder builder = new GcpClientBuilder.BigQueryBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testConfigProjectWhenFlagFalse() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "false");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryBuilder builder = new GcpClientBuilder.BigQueryBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testStorageProjectFromCredentialsFlagTrue() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.GcsBuilder builder = new GcpClientBuilder.GcsBuilder();
    builder.withConfig(config);

    assertEquals("cred_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testStorageConfigProjectWhenCredsMissing() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account_no_project.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.GcsBuilder builder = new GcpClientBuilder.GcsBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testStorageConfigProjectWhenFlagFalse() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "false");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.GcsBuilder builder = new GcpClientBuilder.GcsBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getOptions().getProjectId());
  }

  @Test
  public void testWriteSettingsProjectFromCredentialsFlagTrue() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryWriteSettingsBuilder builder =
        new GcpClientBuilder.BigQueryWriteSettingsBuilder();
    builder.withConfig(config);

    assertEquals("cred_project", builder.build().getQuotaProjectId());
  }

  @Test
  public void testWriteSettingsConfigProjectWhenCredsMissing() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account_no_project.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "true");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryWriteSettingsBuilder builder =
        new GcpClientBuilder.BigQueryWriteSettingsBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getQuotaProjectId());
  }

  @Test
  public void testWriteSettingsConfigProjectWhenFlagFalse() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "config_project");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.KEYFILE_CONFIG, resourcePath("dummy_service_account.json"));
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dataset");
    properties.put(BigQuerySinkConfig.USE_PROJECT_FROM_CREDS_CONFIG, "false");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    GcpClientBuilder.BigQueryWriteSettingsBuilder builder =
        new GcpClientBuilder.BigQueryWriteSettingsBuilder();
    builder.withConfig(config);

    assertEquals("config_project", builder.build().getQuotaProjectId());
  }
}
