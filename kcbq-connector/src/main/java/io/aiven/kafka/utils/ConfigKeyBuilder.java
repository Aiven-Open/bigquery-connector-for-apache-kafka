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

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;

public class ConfigKeyBuilder<T extends ConfigKeyBuilder<?>> {
  protected final String name;
  protected ConfigDef.Type type;
  protected Object defaultValue;
  protected ConfigDef.Validator validator;
  protected ConfigDef.Importance importance;
  protected String documentation;
  protected String group;
  protected int orderInGroup;
  protected ConfigDef.Width width;
  protected String displayName;
  protected Set<String> dependents;
  protected ConfigDef.Recommender recommender;
  protected boolean internalConfig;


  public ConfigKeyBuilder(String name) {
    this.name = name;
    this.type = ConfigDef.Type.STRING;
    this.displayName = name;
    this.defaultValue = NO_DEFAULT_VALUE;
    this.orderInGroup = -1;
    this.width = ConfigDef.Width.NONE;
  }

  public ConfigDef.ConfigKey build() {
    return new ConfigDef.ConfigKey(name, type, defaultValue, validator, importance, documentation, group,
        orderInGroup, width, displayName, getDependents(), recommender, internalConfig);
  }

  @SuppressWarnings("unchecked")
  protected T self() {
    return (T) this;
  }

  public final T type(final ConfigDef.Type type) {
    this.type = type;
    return self();
  }

  public final T defaultValue(final Object defaultValue) {
    this.defaultValue = defaultValue;
    return self();
  }

  public final T validator(final ConfigDef.Validator validator) {
    this.validator = validator;
    return self();
  }

  public final T importance(final ConfigDef.Importance importance) {
    this.importance = importance;
    return self();
  }

  public final T documentation(final String documentation) {
    this.documentation = documentation;
    return self();
  }

  public final T group(final String group) {
    this.group = group;
    return self();
  }

  public final T orderInGroup(final int orderInGroup) {
    this.orderInGroup = orderInGroup;
    return self();
  }

  public final T width(final ConfigDef.Width width) {
    this.width = width;
    return self();
  }

  public final T displayName(final String displayName) {
    this.displayName = displayName;
    return self();
  }

  public final T dependents(final Collection<String> dependents) {
    if (this.dependents == null) {
      this.dependents = new LinkedHashSet<>(dependents);
    } else {
      this.dependents.addAll(dependents);
    }
    return self();
  }

  public final T dependent(final String dependent) {
    if (this.dependents == null) {
      this.dependents = new LinkedHashSet<>();
    }
    this.dependents.add(dependent);
    return self();
  }

  List<String> getDependents() {
    return dependents == null ? Collections.emptyList() : new LinkedList<>(dependents);
  }

  public final T recommender(final ConfigDef.Recommender recommender) {
    this.recommender = recommender;
    return self();
  }

  public final T internalConfig(final boolean internalConfig) {
    this.internalConfig = internalConfig;
    return self();
  }
}


