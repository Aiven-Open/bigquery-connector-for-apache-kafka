package com.wepay.kafka.connect.bigquery.integration;

import io.aiven.kafka.utils.VersionInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VersionTestIT {
    @Test
    void versionTest() throws Exception {
        VersionInfo versionInfo = new VersionInfo();
        assertEquals("Aiven", versionInfo.getVendor());
    }
}
