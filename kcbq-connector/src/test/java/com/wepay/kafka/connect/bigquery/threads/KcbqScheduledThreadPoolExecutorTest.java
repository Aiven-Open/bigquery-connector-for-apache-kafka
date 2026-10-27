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
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.threads;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KcbqScheduledThreadPoolExecutorTest {

  @Test
  void textExecutor() throws InterruptedException {
    AtomicInteger ex1Counter = new AtomicInteger(0);
    AtomicInteger ex2Counter = new AtomicInteger(0);
    StringWriter writer = new StringWriter();
    KcbqScheduledThreadPoolExecutor executor = new KcbqScheduledThreadPoolExecutor();
    NamedRunnable ex1 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex1 - execution ")
                    .append(Integer.toString(ex1Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex1");

    NamedRunnable ex2 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex2 - execution ")
                    .append(Integer.toString(ex2Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex2");
    executor.scheduleWithFixedDelay(ex1, 0, 1, TimeUnit.SECONDS);
    executor.scheduleWithFixedDelay(ex2, 0, 1, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      synchronized (writer) {
        writer.append(executor.status()).append(System.lineSeparator());
      }
      Thread.sleep(1000);
    }

    synchronized (writer) {
      writer.append("Shutting down").append(System.lineSeparator());
    }
    executor.shutdown();

    String result = writer.toString();
    // we should have execution 9 for both executors but because of potential load issues we will
    // only check for 8
    assertThat(result).contains("ex1 - execution 8").contains("ex2 - execution 8");
  }

  @Test
  void textExecutorWithException() throws InterruptedException {
    AtomicInteger ex1Counter = new AtomicInteger(0);
    AtomicInteger ex2Counter = new AtomicInteger(0);
    StringWriter writer = new StringWriter();
    KcbqScheduledThreadPoolExecutor executor = new KcbqScheduledThreadPoolExecutor();
    NamedRunnable ex1 =
        new NamedRunnable(
            () -> {
              if (ex2Counter.get() == 5) {
                throw new RuntimeException("Thrown exception");
              }
              synchronized (writer) {
                writer
                    .append("ex1 - execution ")
                    .append(Integer.toString(ex1Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex1");

    NamedRunnable ex2 =
        new NamedRunnable(
            () -> {
              synchronized (writer) {
                writer
                    .append("ex2 - execution ")
                    .append(Integer.toString(ex2Counter.getAndIncrement()))
                    .append(System.lineSeparator());
              }
            },
            "Ex2");
    executor.scheduleWithFixedDelay(ex1, 0, 1, TimeUnit.SECONDS);
    executor.scheduleWithFixedDelay(ex2, 0, 1, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      synchronized (writer) {
        writer.append(executor.status()).append(System.lineSeparator());
      }
      Thread.sleep(1000);
    }

    synchronized (writer) {
      writer.append("Shutting down").append(System.lineSeparator());
    }
    executor.shutdown();

    String result = writer.toString();
    // we should have execution 9 for both executors but because of potential load issues we will
    // only check for 8
    assertThat(result)
        .contains("ex1 - execution 4")
        .contains("ex2 - execution 9")
        .doesNotContain("ex1 - execution 5");
    assertThat(executor.getLastError()).isPresent();
    assertThat(executor.getLastError().get()).hasMessageContaining("Thrown exception");
  }
}
