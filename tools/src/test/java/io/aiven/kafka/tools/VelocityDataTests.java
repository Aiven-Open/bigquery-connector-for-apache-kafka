/*
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information    *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the      *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                *
 *   http://www.apache.org/licenses/LICENSE-2.0         *
 *                                *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY     *
 * KIND, either express or implied.  See the License for the  *
 * specific language governing permissions and limitations    *
 * under the License.                       *
 */

package io.aiven.kafka.tools;

import io.aiven.kafka.utils.ConfigKeyBuilder;
import io.aiven.kafka.utils.ExtendedConfigKey;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VelocityDataTests {

    @Test
    void testExtendedConfigKey() {
        ExtendedConfigKey extendedConfigKey = ExtendedConfigKey.builder("testOpt").deprecatedInfo(ExtendedConfigKey.DeprecatedInfo.builder()).build();
        VelocityData underTest = new VelocityData(extendedConfigKey);
        assertTrue(underTest.isExtendedFlag());
        assertNotNull(underTest.deprecated());
        assertNull(underTest.since());

        extendedConfigKey = ExtendedConfigKey.builder("testOpt").deprecatedInfo(ExtendedConfigKey.DeprecatedInfo.builder()).since("Then").build();
        underTest = new VelocityData(extendedConfigKey);
        assertTrue(underTest.isExtendedFlag());
        assertNotNull(underTest.deprecated());
        assertEquals("Then", underTest.since());

    }

    @Test
    void testConfigKey() {
        ConfigDef.ConfigKey configKey = new ConfigKeyBuilder<>("testOpt").build();
        VelocityData underTest = new VelocityData(configKey);
        assertFalse(underTest.isExtendedFlag());
        assertNull(underTest.deprecated());
        assertNull(underTest.since());
    }

}
