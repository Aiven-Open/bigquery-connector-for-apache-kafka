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

package io.aiven.kafka.config.tools;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;

/**
 * A Base ConfigDefBean that provides created {@link ConfigKeyBean} instances from {@link ConfigDef.ConfigKey} instances.
 * Also provides convenience methods for formatting and ConfigKeyBean access.
 * <p>
 * DEVHINT: Be careful when removing methods as this may invalidate contents and functionality of the velocity templates.
 * </p>
 *
 * <p>Example of use <pre>{@code
 * @DefaultKey("configDef")
 * @ValidScope({"application"})
 * public class ConfigDefBean extends BaseConfigDefBean<ConfigKeyBean> {
 *   public ConfigDefBean() {
 *     super(myConnectorConfig.getConfigDef(), ConfigKeyBean::new);
 *   }
 * }
 * }</pre>
 * </p>
 *
 * @param <T> The {@link ConfigKeyBean} type that this ConfigDefBean returns.
 */
public class BaseConfigDefBean<T extends ConfigKeyBean> {

  /**
   * Converts a string into an array of single character strings
   *
   * @param charText the text to convert.
   * @return An array of single character strings
   */
  private static String[] charParser(final String charText) {
    char[] chars = charText.toCharArray();
    String[] result = new String[chars.length];
    for (int i = 0; i < chars.length; i++) {
      result[i] = String.valueOf(chars[i]);
    }
    return result;
  }

  /**
   * The characters to escape for markdown.
   */
  private static final String[] MARKDOWN_CHARS = charParser("\\`*_{}[]<>()#+-.!|");
  /**
   * The characters to escape for APT (Almost Plain Text).
   */
  private static final String[] APT_CHARS = charParser("\\~=-+*[]<>{}");

  /**
   * The configuration definition that we are processing.
   */
  private final ConfigDef configDef;

  /**
   * A function to convert a ConfigKey to the bean type we are returning.
   */
  private final Function<? super ConfigDef.ConfigKey, T> constructor;

  /**
   * Constructor.
   */
  public BaseConfigDefBean(ConfigDef configDef, Function<? super ConfigDef.ConfigKey, T> constructor) {
    this.configDef = configDef;
    this.constructor = constructor;
  }

  private List<T> generatedFilteredList(Predicate<ConfigDef.ConfigKey> filter) {
    return configDef.configKeys().values().stream()
            .filter(filter)
            .map(constructor).sorted(Comparator.comparing(ConfigKeyBean::getName)).collect(Collectors.toList());
  }

  /**
   * Finds all the nodes for which {@code name} is a dependent.
   *
   * @param name the name to find the parents for.
   * @return the list of parents.
   */
  public List<T> parents(String name) {
    return generatedFilteredList(c -> c.dependents.contains(name));
  }

  /**
   * Finds all the nodes that have dependents.
   *
   * @return the list of parents.
   */
  public List<T> parents() {
    return generatedFilteredList(c -> !c.dependents.isEmpty());
  }

  /**
   * Finds all the names that have been listed in a dependent field.
   *
   * @return the list of all dependent nodes.
   */
  public List<T> dependents() {
    Set<String> dependentNames = new HashSet<>();
    configDef.configKeys().values().stream()
            .filter(c -> c.dependents != null)
            .forEach(c -> dependentNames.addAll(c.dependents));
    return generatedFilteredList(c -> dependentNames.contains(c.name));
  }

  /**
   * Gets the list of configuration options.
   *
   * @return the list of configuration options.
   */
  public List<T> configKeys() {
    return configDef.configKeys().values().stream().map(constructor).sorted(Comparator.comparing(ConfigKeyBean::getName)).collect(Collectors.toList());
  }

  /**
   * Escapes a text string.
   *
   * @param text  the text to escape.
   * @param chars the characters to escape.
   * @return the escaped string.
   */
  private String escape(final String text, final String[] chars) {
    if (text == null) {
      return "";
    }
    String result = text;
    for (String c : chars) {
      result = result.replace(c, "\\" + c);
    }
    return result;
  }

  /**
   * Escapes a string for markdown.
   *
   * @param text the text to escape.
   * @return the text with the markdown specific characters escaped.
   */
  @SuppressWarnings("unused")
  public final String markdownEscape(final String text) {
    return escape(text, MARKDOWN_CHARS);
  }

  /**
   * Escapes a string for APT (almost plain text).
   *
   * @param text the text to escape.
   * @return the text with the APT specific characters escaped.
   */
  @SuppressWarnings("unused")
  public final String aptEscape(final String text) {
    return escape(text, APT_CHARS);
  }

  /**
   * Gets a tab character.
   *
   * @return the tab character.
   */
  @SuppressWarnings("unused")
  public final String tab() {
    return "\t";
  }

  /**
   * Gets two new lines.
   *
   * @return a string containing two new lines.
   */
  @SuppressWarnings("unused")
  public final String doubleLine() {
    return "\n\n";
  }

  /**
   * Creates a string of spaces of the specified length.
   *
   * @param length the length of the string.
   * @return a string of spaces of the specified length.
   */
  @SuppressWarnings("unused")
  public final String pad(final int length) {
    char[] padding = new char[length];
    Arrays.fill(padding, ' ');
    return new String(padding);
  }
}