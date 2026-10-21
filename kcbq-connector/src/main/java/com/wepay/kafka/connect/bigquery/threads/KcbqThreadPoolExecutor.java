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

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.write.batch.CountDownRunnable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThreadPoolExecutor for writing Rows to BigQuery.
 *
 * <p>Keeps track of the number of threads actively writing for each topic. Keeps track of the
 * number of failed threads in each batch of requests.
 */
public class KcbqThreadPoolExecutor extends ThreadPoolExecutor implements KcbqExecutor {

  private static final Logger logger = LoggerFactory.getLogger(KcbqThreadPoolExecutor.class);

  private final AtomicReference<Throwable> encounteredError = new AtomicReference<>();

  /**
   * Construct an executor with the maximum pool size or 5 whichever is larger.
   * @param maxPoolSize The maximum pools size should be 5 or greater.
   */
  public KcbqThreadPoolExecutor(int maxPoolSize) {
    // Since queue is unbounded the maximum number of threads that will be created is the corePoolSize
    super(
            Math.max(maxPoolSize, 5),
            Math.max(maxPoolSize, 5),
        10,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new MdcContextThreadFactory());
    // allow unused core threads to be removed.
    this.allowCoreThreadTimeOut(true);
  }

  @Override
  public String status() {
    return String.format(
        "ThreadPool pool:%s active:%s queue:%s task:%s completed:%s",
        getPoolSize(), getActiveCount(), getQueue().size(), getTaskCount(), getCompletedTaskCount());
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

    if (throwable != null) {
      logAndRecord(throwable);
    }
  }

  @Override
  public void logAndRecord(Throwable throwable) {
    if (!(throwable instanceof ExpectedInterruptException)) {
      // Log at debug level since this will be shown to the user at error level by the Connect
      // framework if it causes
      // the task to fail, and will otherwise just pollute logs and potentially mislead users
      logger.debug("A write thread has failed with an unrecoverable error.", throwable);
      logger.debug("Error preserved: {}", encounteredError.compareAndSet(null, throwable));
    }
  }

  @Override
  public void maybeThrowEncounteredError() {
    Throwable t = encounteredError.get();
    if (t != null) {
      throw new BigQueryConnectException(
          "A write thread has failed with an unrecoverable error: " + t.getMessage(), t);
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
}
