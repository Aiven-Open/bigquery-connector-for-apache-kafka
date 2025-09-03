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

package io.aiven.kafka.tools;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.velocity.tools.config.DefaultKey;
import org.apache.velocity.tools.config.ValidScope;

/**
 * The Velocity RAT plugin that provides access to the RAT data.
 * <p>
 * DEVHINT: Be careful when removing methods as this may invalidate contents and functionality of the velocity templates.
 * </p>
 */
@SuppressWarnings("unused")
@DefaultKey("bigquery")
@ValidScope({"application"})
public class VelocityTool {

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

  private final ConfigDef configDef;

  /**
   * Constructor.
   */
  public VelocityTool() {
    this.configDef = BigQuerySinkConfig.getConfig();
  }

  /**
   * Gets the list of configuration options.
   *
   * @return the list of configuration options.
   */
  public List<VelocityData> options() {
    return configDef.configKeys().values().stream().map(VelocityData::new).sorted(Comparator.comparing(VelocityData::getName)).collect(Collectors.toList());
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
  public String markdownEscape(final String text) {
    return escape(text, MARKDOWN_CHARS);
  }

  /**
   * Escapes a string for APT (almost plain text).
   *
   * @param text the text to escape.
   * @return the text with the APT specific characters escaped.
   */
  public String aptEscape(final String text) {
    return escape(text, APT_CHARS);
  }


  /**
   * Gets the {@link StringUtils} object in order to work with it in Velocity templates.
   *
   * @return the org.apache.commons.lang3 StringUtils object.
   * @see org.apache.commons.lang3.StringUtils
   */
  public StringUtils stringUtils() {
    return new StringUtils();
  }

  /**
   * Gets a tab character.
   *
   * @return the tab character.
   */
  public String tab() {
    return "\t";
  }

  /**
   * Gets two new lines.
   *
   * @return a string containing two new lines.
   */
  public String doubleLine() {
    return "\n\n";
  }


  /**
   * Creates a string of spaces of the specified length.
   *
   * @param length the length of the string.
   * @return a string of spaces of the specified length.
   */
  public String pad(final int length) {
    char[] padding = new char[length];
    Arrays.fill(padding, ' ');
    return new String(padding);
  }
}