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

package io.aiven.kafka.utils;

import java.util.function.Supplier;
import org.apache.kafka.common.config.ConfigDef;

public class ExtendedConfigKey extends ConfigDef.ConfigKey {

  /**
   * The deprecation information. May be {@code null}.
   */
  public final DeprecatedInfo deprecated;

  /**
   * The version in which this attribute was added. May be {@code null}.
   */
  public final String since;

  public ExtendedConfigKey(ExtendedConfigKey.Builder<?> builder) {
    super(builder.name, builder.type, builder.defaultValue, builder.validator, builder.importance, builder.documentation, builder.group,
        builder.orderInGroup, builder.width, builder.displayName, builder.getDependents(), builder.recommender, builder.internalConfig);
    this.deprecated = builder.deprecated;
    this.since = builder.since;
  }

  public final String getDeprecationMessage() {
    return isDeprecated() ? deprecated.description : "";
  }

  public final String getSince() {
    return since;
  }

  public final boolean isDeprecated() {
    return deprecated != null;
  }

  public static <T extends ExtendedConfigKey.Builder<?>> ExtendedConfigKey.Builder<T> builder(String name) {
    return new Builder<>(name);
  }


  public static class Builder<T extends ExtendedConfigKey.Builder<?>> extends ConfigKeyBuilder<T> {
    private DeprecatedInfo deprecated;
    private String since;

    protected Builder(String name) {
      super(name);
    }

    @Override
    public ExtendedConfigKey build() {
      return new ExtendedConfigKey(this);
    }

    public final T deprecatedInfo(final DeprecatedInfo.Builder deprecatedInfoBuilder) {
      return deprecatedInfo(deprecatedInfoBuilder.get());
    }

    public final T deprecatedInfo(final DeprecatedInfo deprecatedInfo) {
      this.deprecated = deprecatedInfo;
      return self();
    }

    public final T since(final String since) {
      this.since = since;
      return self();
    }
  }

  public static class DeprecatedInfo {
    /**
     * Builds {@link DeprecatedInfo}.
     */
    public static class Builder implements Supplier<DeprecatedInfo> {

      /**
       * The description.
       */
      private String description;

      /**
       * Whether this option is subject to removal in a future version.
       *
       * @see <a href="https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Deprecated.html#forRemoval()">Deprecated.forRemoval</a>
       */
      private boolean forRemoval;

      /**
       * The version in which the option became deprecated.
       *
       * @see <a href="https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Deprecated.html#forRemoval()">Deprecated.since</a>
       */
      private String since;


      @Override
      public DeprecatedInfo get() {
        return new DeprecatedInfo(description, since, forRemoval);
      }

      /**
       * Sets the description.
       *
       * @param description the description.
       * @return {@code this} instance.
       */
      public Builder setDescription(final String description) {
        this.description = description;
        return this;
      }

      /**
       * Sets whether this option is subject to removal in a future version.
       *
       * @param forRemoval whether this is subject to removal in a future version.
       * @return {@code this} instance.
       * @see <a href="https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/Deprecated.html#forRemoval()">Deprecated.forRemoval</a>
       */
      public Builder setForRemoval(final boolean forRemoval) {
        this.forRemoval = forRemoval;
        return this;
      }

      /**
       * Sets the version in which the option became deprecated.
       *
       * @param since the version in which the option became deprecated.
       * @return {@code this} instance.
       */
      public Builder setSince(final String since) {
        this.since = since;
        return this;
      }
    }

    /**
     * Creates a new builder.
     *
     * @return a new builder.
     */
    public static Builder builder() {
      return new Builder();
    }

    /**
     * The description.
     */
    private final String description;

    /**
     * Whether this option will be removed.
     */
    private final boolean forRemoval;

    /**
     * The version label for removal.
     */
    private final String since;

    /**
     * Constructs a new instance.
     *
     * @param description The description.
     * @param since     The version label for removal.
     * @param forRemoval  Whether this option will be removed.
     */
    private DeprecatedInfo(final String description, final String since, final boolean forRemoval) {
      this.description = toEmpty(description);
      this.since = toEmpty(since);
      this.forRemoval = forRemoval;
    }

    /**
     * Gets the descriptions.
     *
     * @return the descriptions.
     */
    public String getDescription() {
      return description;
    }

    /**
     * Gets version in which the option became deprecated.
     *
     * @return the version in which the option became deprecated.
     */
    public String getSince() {
      return since;
    }

    /**
     * Tests whether this option is subject to removal in a future version.
     *
     * @return whether this option is subject to removal in a future version.
     */
    public boolean isForRemoval() {
      return forRemoval;
    }

    private String toEmpty(final String since) {
      return since != null ? since : "";
    }

    @Override
    public String toString() {
      return formatted(null);
    }

    /**
     * Gets a string that contains the deprecated information.  The output will read:
     * "{@code name} deprecated for removal since {@link #since}: {@link #description}".  Where
     * <ul>
     * <li>{@code name} is the name argument.  If {@code name} is empty, the start of the result will be "Deprecated"</li>
     * <li>"for removal" will not be present if {@link #forRemoval} is {@code false}.</li>
     * <li>"since {@link #since}" will contain the {@link #since} text, or be omitted if {@link #since} is not set.</li>
     * <li>": {@link #description}" will contain the {@link #description} text, or be omitted if  {@link #description} is empty.</li>
     * </ul>
     *
     * @param name The name to use for the formatted display.
     * @return the formatted information.
     */
    public String formatted(String name) {
      final StringBuilder builder = name == null || name.isEmpty() ? new StringBuilder("Deprecated") : new StringBuilder(name).append(" is deprecated");
      if (forRemoval) {
        builder.append(" for removal");
      }
      if (!since.isEmpty()) {
        builder.append(" since ");
        builder.append(since);
      }
      if (!description.isEmpty()) {
        builder.append(": ");
        builder.append(description);
      }
      return builder.toString();
    }
  }
}
