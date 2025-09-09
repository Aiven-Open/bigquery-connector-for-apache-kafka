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

import io.aiven.kafka.utils.ExtendedConfigKey;
import org.apache.kafka.common.config.ConfigDef;


/**
* Defines the variables that are available for the Velocity template to access a {@link ConfigDef.ConfigKey} object.
   *
       * @see <a href="https://velocity.apache.org/">Apache Velocity</a>
    */
public class ExtendedConfigKeyBean extends ConfigKeyBean {
  /** The key */
  private final boolean extendedFlag;

  /**
   * Constructor.
   *
   * @param key
   *      the Key to wrap.
   */
  public ExtendedConfigKeyBean(final ConfigDef.ConfigKey key) {
    super(key);
    this.extendedFlag = (key instanceof ExtendedConfigKey);
  }

  public final boolean isExtendedFlag() {
    return extendedFlag;
  }

  private ExtendedConfigKey asExtended() {
    return extendedFlag ? (ExtendedConfigKey) key : null;
  }

  public final String since() {
    return extendedFlag ? asExtended().since : null;
  }

  @SuppressWarnings("unused")
  public final boolean isDeprecated() {
    return extendedFlag && asExtended().isDeprecated();
  }

  public final ExtendedConfigKey.DeprecatedInfo deprecated() {
    return extendedFlag ? asExtended().deprecated : null;
  }

}
