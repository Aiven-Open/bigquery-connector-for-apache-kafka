package com.wepay.kafka.connect.bigquery.write.storage;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class BigQueryWriteSettingsBuilderTest {

  @Test
  public void testBigQueryWriteSettingsBuild() {
    Map<String, String> properties = new HashMap<>();
    properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "abcd");
    properties.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, GcpClientBuilder.KeySource.FILE.name());
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "dummy_dataset");
    BigQuerySinkConfig config = new BigQuerySinkConfig(properties);

    BigQueryWriteSettings actualSettings = new GcpClientBuilder.BigQueryWriteSettingsBuilder()
        .withConfig(config)
        .build();

    assertEquals(actualSettings.getQuotaProjectId(), "abcd");
  }

}
