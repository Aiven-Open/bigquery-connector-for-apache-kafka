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

package com.wepay.kafka.connect.bigquery.convert.logicaltype;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for finding and accessing {@link LogicalTypeConverter}s.
 */
public class LogicalConverterRegistry {

  private static Map<String, LogicalTypeConverter> converterMap = new ConcurrentHashMap<>();

  /**
   * Registers the logical type name.  Will override existing value if any.
   *
   * @param logicalTypeName the logical type name to register.
   * @param converter the converter for the name.  May not be {@code null}.
   */
  public static void register(String logicalTypeName, LogicalTypeConverter converter) {
    converterMap.put(logicalTypeName, converter);
  }

  /**
   * Registers the logical type name if it was not previously registered.
   *
   * @param logicalTypeName the logical type name to register.
   * @param converter the converter for the name.  May not be {@code null}.
   */
  public static void registerIfAbsent(String logicalTypeName, LogicalTypeConverter converter) {
    converterMap.putIfAbsent(logicalTypeName, converter);
  }

  /**
   * Unregisters (removes) the logical type name if it was previously registered.  After an {@code unregister} call
   * the result of {@link #isRegisteredLogicalType(String)} is guaranteed to be false.
   *
   * @param logicalTypeName the logical type name to unregister.
   */
  public static void unregister(String logicalTypeName) {
    if (logicalTypeName != null) {
      converterMap.remove(logicalTypeName);
    }
  }

  /**
   * Gets the converter registered with the logical type name.
   *
   * @param logicalTypeName the logical type name. May be {@code null}.
   * @return the LogicalTypeConverter or {@code null} if none is registered or {@code null} passed for {@code logicalTypeName}.
   */
  public static LogicalTypeConverter getConverter(String logicalTypeName) {
    return logicalTypeName == null ? null : converterMap.get(logicalTypeName);
  }

  /**
   * Determines if a converter is registered with the logical type name.
   *
   * @param typeName the logical type name.
   * @return {@code true} if there is a converter registered, {@code false} otherwise.
   */
  public static boolean isRegisteredLogicalType(String typeName) {
    return typeName != null && converterMap.containsKey(typeName);
  }
}
