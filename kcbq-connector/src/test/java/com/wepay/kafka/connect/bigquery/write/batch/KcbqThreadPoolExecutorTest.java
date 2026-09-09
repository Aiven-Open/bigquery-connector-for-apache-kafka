/*
 * Copyright 2026 Copyright 2022 Aiven Oy and
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

package com.wepay.kafka.connect.bigquery.write.batch;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wepay.kafka.connect.bigquery.SinkTaskPropertiesFactory;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class KcbqThreadPoolExecutorTest {

  private final CountDownLatch hang = new CountDownLatch(1);
  private KcbqThreadPoolExecutor executor;

  private KcbqThreadPoolExecutor executor(long flushTimeoutMs) {
    Map<String, String> properties = new SinkTaskPropertiesFactory().getProperties();
    properties.put(BigQuerySinkTaskConfig.THREAD_POOL_SIZE_CONFIG, "2");
    properties.put(BigQuerySinkTaskConfig.FLUSH_TIMEOUT_MS_CONFIG, Long.toString(flushTimeoutMs));
    executor =
        new KcbqThreadPoolExecutor(
            new BigQuerySinkTaskConfig(properties), new LinkedBlockingQueue<>(), Thread::new);
    return executor;
  }

  /** Submits a write task that never completes, standing in for a hung write call. */
  private void submitHungWriteTask(KcbqThreadPoolExecutor executor) {
    executor.execute(
        () -> {
          try {
            hang.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
  }

  @AfterEach
  public void tearDown() {
    hang.countDown();
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Test
  public void testAwaitCurrentTasksBlocksIndefinitelyOnHungWriteTaskByDefault() throws Exception {
    KcbqThreadPoolExecutor executor = executor(0L);
    submitHungWriteTask(executor);

    CountDownLatch returned = new CountDownLatch(1);
    Thread flusher =
        new Thread(
            () -> {
              try {
                executor.awaitCurrentTasks();
              } catch (Exception e) {
                // fall through -- either way the call came back
              }
              returned.countDown();
            });
    flusher.setDaemon(true);
    flusher.start();

    assertFalse(
        returned.await(1, TimeUnit.SECONDS),
        "awaitCurrentTasks() should block while a write task is in flight");
    flusher.interrupt();
  }

  @Test
  public void testAwaitCurrentTasksThrowsAfterFlushTimeoutOnHungWriteTask() {
    KcbqThreadPoolExecutor executor = executor(500L);
    submitHungWriteTask(executor);

    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> assertThrows(BigQueryConnectException.class, executor::awaitCurrentTasks));
  }

  @Test
  public void testPoolRecoversAfterFlushTimeout() throws Exception {
    KcbqThreadPoolExecutor executor = executor(500L);
    submitHungWriteTask(executor);
    assertThrows(BigQueryConnectException.class, executor::awaitCurrentTasks);

    // The timed-out flush must not leave its barrier behind: no CountDownRunnable stays queued,
    // and the pool thread not occupied by the hung write is free to run new work.
    assertTrue(executor.getQueue().stream().noneMatch(r -> r instanceof CountDownRunnable));
    CountDownLatch ran = new CountDownLatch(1);
    executor.execute(ran::countDown);
    assertTrue(ran.await(5, TimeUnit.SECONDS), "new write task did not run after a flush timeout");

    // A later flush with the write still hung times out again instead of blocking forever.
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> assertThrows(BigQueryConnectException.class, executor::awaitCurrentTasks));
  }

  @Test
  public void testInterruptedFlushReleasesBarrier() throws Exception {
    KcbqThreadPoolExecutor executor = executor(60_000L);
    submitHungWriteTask(executor);
    CountDownLatch interrupted = new CountDownLatch(1);
    Thread flusher =
        new Thread(
            () -> {
              try {
                executor.awaitCurrentTasks();
              } catch (InterruptedException e) {
                interrupted.countDown();
              } catch (Exception e) {
                // not expected
              }
            });
    flusher.setDaemon(true);
    flusher.start();
    Thread.sleep(200);
    flusher.interrupt();
    assertTrue(interrupted.await(5, TimeUnit.SECONDS));

    assertTrue(executor.getQueue().stream().noneMatch(r -> r instanceof CountDownRunnable));
    CountDownLatch ran = new CountDownLatch(1);
    executor.execute(ran::countDown);
    assertTrue(ran.await(5, TimeUnit.SECONDS), "new write task did not run after an interrupt");
  }

  @Test
  public void testAwaitCurrentTasksCompletesNormallyWithinFlushTimeout() {
    KcbqThreadPoolExecutor executor = executor(5000L);
    AtomicBoolean ran = new AtomicBoolean();
    executor.execute(() -> ran.set(true));

    assertTimeoutPreemptively(Duration.ofSeconds(10), () -> executor.awaitCurrentTasks());
    assertTrue(ran.get());
  }
}
