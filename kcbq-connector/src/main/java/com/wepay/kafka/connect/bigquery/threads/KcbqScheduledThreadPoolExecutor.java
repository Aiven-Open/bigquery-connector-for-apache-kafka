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

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A scheduled thread pool. Only one thread at a time is supported.  This pool should be used to schedule tasks that do not take too long.
 * As one long running task may block others.
 */
public class KcbqScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
    implements KcbqExecutor {

  private static final Logger logger =
      LoggerFactory.getLogger(KcbqScheduledThreadPoolExecutor.class);

  private final AtomicReference<Throwable> encounteredError = new AtomicReference<>();

  public KcbqScheduledThreadPoolExecutor() {
    super(1, new MdcContextThreadFactory());
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    if (logger.isDebugEnabled()) {
      logger.debug(status());
    }
  }

  @Override
  protected void afterExecute(Runnable runnable, Throwable throwable) {
    super.afterExecute(runnable, throwable);
    if (runnable instanceof FutureTask<?> future) {
      try {
        if (future.isDone()) {
          future.get();
        }
      } catch (Exception e) {
        logAndRecord(e);
      }
    } else if (throwable != null) {
      logAndRecord(throwable);
    }
  }

  @Override
  public void logAndRecord(Throwable throwable) {
    if (!(throwable instanceof ExpectedInterruptException)) {
      logger.warn(
          "A scheduled thread has failed with an unrecoverable error. " + throwable.getMessage(),
          throwable);
      logger.debug("Error preserved: {}", encounteredError.compareAndSet(null, throwable));
    }
  }

  @Override
  public void maybeThrowEncounteredError() {
    Throwable t = encounteredError.get();
    if (t != null) {
      throw new BigQueryConnectException(
          "A scheduled thread has failed with an unrecoverable error: " + t.getMessage(), t);
    }
  }

  @Override
  public Optional<Throwable> getLastError() {
    return Optional.ofNullable(encounteredError.get());
  }

  @Override
  public List<String> taskNames() {
    return getQueue().stream().map(Object::toString).toList();
  }

  @Override
  public String status() {
    return String.format(
        "ScheduledThreadPool pool:%s active:%s queue:%s task:%s completed:%s",
        getPoolSize(), getActiveCount(), getQueue().size(), getTaskCount(), getCompletedTaskCount());
  }
}

