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

package com.wepay.kafka.connect.bigquery.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

public class MultiPropertyValidatorTest {

  @Test
  public void testExistingErrorSkipsValidation() {
    MultiPropertyValidator<Object> validator = new TestValidator<>(
        "p",
        Arrays.asList("d1", "d2", "d3"),
        o -> {
          fail("Validation should have been performed on property that already has an error");
          return null;
        }
    );

    ConfigValue configValue = new ConfigValue("p", "v", Collections.emptyList(), Collections.singletonList("an error"));

    assertEquals(
        Optional.empty(),
        validator.validate(configValue, null, Collections.emptyMap())
    );
  }

  @Test
  public void testDependentErrorSkipsValidation() {
    MultiPropertyValidator<Object> validator = new TestValidator<>(
        "p",
        Arrays.asList("d1", "d2", "d3"),
        o -> {
          fail("Validation should have been performed on property whose dependent already has an error");
          return null;
        }
    );

    ConfigValue configValue = new ConfigValue("p", "v", Collections.emptyList(), Collections.emptyList());
    Map<String, ConfigValue> valuesByName = ImmutableMap.of(
        "d1", new ConfigValue("d1", "v1", Collections.emptyList(), Collections.emptyList()),
        "d2", new ConfigValue("d2", "v1", Collections.emptyList(), Collections.singletonList("an error"))
    );

    assertEquals(
        Optional.empty(),
        validator.validate(configValue, null, valuesByName)
    );
  }

  @Test
  public void testValidationFails() {
    Optional<String> expectedError = Optional.of("an error");
    MultiPropertyValidator<Object> validator = new TestValidator<>(
        "p",
        Collections.emptyList(),
        o -> expectedError
    );

    ConfigValue configValue = new ConfigValue("p", "v", Collections.emptyList(), Collections.emptyList());

    assertEquals(
        expectedError,
        validator.validate(configValue, null, Collections.emptyMap())
    );
  }

  @Test
  public void testUnexpectedErrorDuringValidation() {
    MultiPropertyValidator<Object> validator = new TestValidator<>(
        "p",
        Collections.emptyList(),
        o -> {
          throw new RuntimeException("Some unexpected error");
        }
    );

    ConfigValue configValue = new ConfigValue("p", "v", Collections.emptyList(), Collections.emptyList());

    assertNotEquals(
        Optional.empty(),
        validator.validate(configValue, null, Collections.emptyMap())
    );
  }

  private static class TestValidator<Config> extends MultiPropertyValidator<Config> {

    private final List<String> dependents;
    private final Function<Config, Optional<String>> validationFunction;

    public TestValidator(String propertyName, List<String> dependents, Function<Config, Optional<String>> validationFunction) {
      super(propertyName);
      this.dependents = dependents;
      this.validationFunction = validationFunction;
    }

    @Override
    protected Collection<String> dependents() {
      return dependents;
    }

    @Override
    protected Optional<String> doValidate(Config config) {
      return validationFunction.apply(config);
    }
  }
}
