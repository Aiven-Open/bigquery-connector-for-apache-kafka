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

package com.wepay.kafka.connect.bigquery.threads;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import org.slf4j.MDC;

/**
 * A thread factory that sets the Mapped Diagnostic Context (MDC) that can be used by SLF4J.
 *
 * @see <a href="https://logback.qos.ch/manual/mdc.html">Mapped Diagnostic Context</a>
 */
public class MdcContextThreadFactory implements ThreadFactory {

  private final Map<String, String> mdcContext;

  public MdcContextThreadFactory() {
    this.mdcContext = MDC.getCopyOfContextMap();
  }

  @Override
  public Thread newThread(Runnable runnable) {
    if (mdcContext == null) {
      return new Thread(runnable);
    } else {
      return new Thread(
          () -> {
            MDC.setContextMap(mdcContext);
            runnable.run();
          });
    }
  }
}
