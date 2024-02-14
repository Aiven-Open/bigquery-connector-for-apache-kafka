package com.wepay.kafka.connect.bigquery.integration;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.util.Map;

@Category(IntegrationTest.class)
public class StorageWriteApiBatchBigQuerySinkConnectorIT extends StorageWriteApiBigQuerySinkConnectorIT {

    @Override
    protected Map<String, String> configs(String topic) {
        Map<String, String> result = super.configs(topic);
        result.put(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG, "true");
        return result;
    }

}
