/*
 * Copyright 2022-2026 Aiven Oy and
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
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.config;

import static java.lang.String.format;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigValue;

public abstract class MultiPropertyValidator<ConfigT> {

  private final String propertyName;

  protected MultiPropertyValidator(String propertyName) {
    this.propertyName = propertyName;
  }

  public String propertyName() {
    return propertyName;
  }

  public Optional<String> validate(
      ConfigValue value, ConfigT config, Map<String, ConfigValue> valuesByName) {
    // Only perform follow-up validation if the property doesn't already have an error associated
    // with it
    if (!value.errorMessages().isEmpty()) {
      return Optional.empty();
    }

    boolean dependentsAreValid =
        dependents().stream()
            .map(valuesByName::get)
            .filter(Objects::nonNull)
            .map(ConfigValue::errorMessages)
            .allMatch(List::isEmpty);
    // Also ensure that all of the other properties that the validation for this one depends on
    // don't already have errors
    if (!dependentsAreValid) {
      return Optional.empty();
    }

    try {
      return doValidate(config);
    } catch (RuntimeException e) {
      return Optional.of(
          "An unexpected error occurred during validation"
              + (e.getMessage() != null ? ": " + e.getMessage() : ""));
    }
  }

  /**
   * Creates a validation message.
   *
   * @param name the name of the configuration property.
   * @param value the value associated with that property.
   * @param message additional info May be {@code null}.
   * @return A formatted validatio nmessage.
   */
  protected static String validationMessage(
      final String name, final Object value, final String message) {
    return format(
        "Invalid value %s for configuration %s%s.",
        value, name, message == null ? "" : ": " + message);
  }

  /**
   * Registers an issue in the Config map.
   *
   * @param configMap The map of name to ConfigValue.
   * @param name the name of the item with the error.
   * @param value the value of the item.
   * @param message the message for the error.
   */
  protected void registerIssue(
      final Map<String, ConfigValue> configMap,
      final String name,
      final Object value,
      final String message) {
    configMap.get(name).addErrorMessage(validationMessage(name, value, message));
  }

  protected abstract Collection<String> dependents();

  protected abstract Optional<String> doValidate(ConfigT config);
}
