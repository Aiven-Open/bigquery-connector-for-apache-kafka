package io.aiven.kafka.tools;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class VelociytToolTests {

    VelocityTool underTest = new VelocityTool();

    @Test
    public void testParents() {
        List<String> expected = Arrays.asList(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, BigQuerySinkConfig.DELETE_ENABLED_CONFIG, BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG);
        VelocityTool underTest = new VelocityTool();
        List<VelocityData> parents = underTest.parents();
        Collections.sort(expected);
        assertEquals(expected, parents.stream().map(VelocityData::getName).collect(Collectors.toList()));

        parents = underTest.parents(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG);

        assertEquals(2, parents.size());
        expected = Arrays.asList(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG, BigQuerySinkConfig.DELETE_ENABLED_CONFIG);
        Collections.sort(expected);
        assertEquals(expected, parents.stream().map(VelocityData::getName).collect(Collectors.toList()));
    }

    @Test
    public void testDependents() {
        List<String> expected = Arrays.asList(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG, BigQuerySinkConfig.INTERMEDIATE_TABLE_SUFFIX_CONFIG, BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG,
                BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG, BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG, BigQuerySinkConfig.KAFKA_KEY_FIELD_NAME_CONFIG);
        VelocityTool underTest = new VelocityTool();
        List<VelocityData> deps = underTest.dependents();
        Collections.sort(expected);
        assertEquals(expected, deps.stream().map(VelocityData::getName).collect(Collectors.toList()));
    }

    @Test
    public void options() {
        VelocityTool underTest = new VelocityTool();
        List<VelocityData> opts =  underTest.options();
        assertNotNull(opts);
    }

}
