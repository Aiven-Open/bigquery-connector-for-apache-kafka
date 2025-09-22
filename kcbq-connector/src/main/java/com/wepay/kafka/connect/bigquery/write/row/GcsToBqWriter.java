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

package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.BaseServiceException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.GcsConnectException;
import com.wepay.kafka.connect.bigquery.utils.GsonUtils;
import com.wepay.kafka.connect.bigquery.utils.Time;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.function.Supplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for batch writing list of rows to BigQuery through GCS.
 */
public class GcsToBqWriter {
  public static final String GCS_METADATA_TABLE_KEY = "sinkTable";
  private static final Logger logger = LoggerFactory.getLogger(GcsToBqWriter.class);
  private static final int WAIT_MAX_JITTER = 1000;
  private static final long MAX_BACKOFF_MS = 10_000L;
  private static final Random random = new Random();
  private static final Gson gson = GsonUtils.SAFE_GSON;
  private final Storage storage;
  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final boolean attemptSchemaUpdate;
  private final Time time;
  private int retries;
  private long retryWaitMs;
  private boolean autoCreateTables;

  /**
   * Initializes a batch GCS writer with a full list of rows to write.
   *
   * @param storage GCS Storage
   * @param bigQuery {@link BigQuery} Object used to perform upload
   * @param retries Maximum number of retry attempts after the initial call.
   *                <p>Note that this is an upper bound: the actual number of retries may be lower if the
   *                computed exponential backoff delays (based on {@code retryWaitMs}) plus jitter cause the
   *                total timeout budget to be exceeded.</p>
   * @param retryWaitMs Base wait time in milliseconds before the first retry. Each subsequent retry
   *                    doubles this delay, up to a maximum per-sleep cap of {@value #MAX_BACKOFF_MS} milliseconds.
   * @param time used to wait during backoff periods
   */
  public GcsToBqWriter(Storage storage,
                       BigQuery bigQuery,
                       SchemaManager schemaManager,
                       int retries,
                       long retryWaitMs,
                       boolean autoCreateTables,
                       boolean attemptSchemaUpdate,
                       Time time) {
    this.storage = storage;
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.attemptSchemaUpdate = attemptSchemaUpdate;
    this.time = time;

    this.retries = retries;
    this.retryWaitMs = retryWaitMs;
    this.autoCreateTables = autoCreateTables;
  }

  private static Map<String, String> getMetadata(TableId tableId) {
    StringBuilder sb = new StringBuilder();
    if (tableId.getProject() != null) {
      sb.append(tableId.getProject()).append(":");
    }
    String serializedTableId =
        sb.append(tableId.getDataset()).append(".").append(tableId.getTable()).toString();
    Map<String, String> metadata =
        Collections.singletonMap(GCS_METADATA_TABLE_KEY, serializedTableId);
    return metadata;
  }

  /**
   * Write rows to BQ through GCS.
   *
   * @param rows       the rows to write.
   * @param tableId    the BQ table to write to.
   * @param bucketName the GCS bucket to write to.
   * @param blobName   the name of the GCS blob to write.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(SortedMap<SinkRecord, RowToInsert> rows,
                        TableId tableId,
                        String bucketName,
                        String blobName) throws InterruptedException {

    // Compute a time budget that still guarantees at least one attempt even if retryWaitMs == 0.
    Duration timeout = Duration.ofMillis(Math.max(0L, retryWaitMs * Math.max(1, retries)));
    if (retries == 0) {
      timeout = Duration.ofMillis(retryWaitMs);
    }

    List<SinkRecord> sinkRecords = new ArrayList<>(rows.keySet());

    // Check if the table specified exists
    // This error shouldn't be thrown. All tables should be created by the connector at startup
    Table table = executeWithRetry(() -> bigQuery.getTable(tableId), timeout);
    boolean lookupSuccess = table != null;

    if (autoCreateTables && !lookupSuccess) {
      logger.info("Table {} was not found. Creating the table automatically.", tableId);
      Boolean created =
          executeWithRetry(
              () -> schemaManager.createTable(tableId, sinkRecords), timeout);
      if (created == null || !created) {
        throw new BigQueryConnectException("Failed to create table " + tableId);
      }
      table = executeWithRetry(() -> bigQuery.getTable(tableId), timeout);
      lookupSuccess = table != null;
    }

    if (!lookupSuccess) {
      throw new BigQueryConnectException("Failed to lookup table " + tableId);
    }

    if (attemptSchemaUpdate && schemaManager != null && !sinkRecords.isEmpty()) {
      Boolean schemaUpdated =
          executeWithRetry(
              () -> {
                schemaManager.updateSchema(tableId, sinkRecords);
                return Boolean.TRUE;
              },
              timeout
          );
      if (schemaUpdated == null) {
        throw new ConnectException(
            String.format("Failed to update schema for table %s within %d re-attempts.", tableId, retries)
        );
      }
    }
    
    // --- Upload rows to GCS with executeWithRetry (fresh budget for uploads) ---
    Duration uploadTimeout = Duration.ofMillis(Math.max(0L, retryWaitMs * Math.max(1, retries)));
    if (retries == 0) {
      uploadTimeout = Duration.ofMillis(retryWaitMs);
    }

    // Get Source URI
    BlobId blobId = BlobId.of(bucketName, blobName);
    Map<String, String> metadata = getMetadata(tableId);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/json").setMetadata(metadata).build();

    Boolean uploaded =
        executeWithRetry(
          () -> {
            // Perform GCS upload; throws StorageException (BaseServiceException) on failure
            uploadRowsToGcs(rows, blobInfo);
            return Boolean.TRUE;
            },
            uploadTimeout
        );

    // If executeWithRetry timed out (budget exhausted) it returns null → fail like before
    if (uploaded == null) {
      throw new ConnectException(
        String.format("Failed to load %d rows into GCS within %d re-attempts.", rows.size(), retries)
      );
    }

    logger.info("Batch loaded {} rows", rows.size());
  }

  /**
   * Creates a JSON string containing all records and uploads it as a blob to GCS.
   *
   * <p>Returns normally on success; throws on failure.</p>
   *
   * @throws com.google.cloud.storage.StorageException if the GCS write fails
   * @throws com.wepay.kafka.connect.bigquery.exception.GcsConnectException if UTF-8 encoding fails
   */
  private void uploadRowsToGcs(SortedMap<SinkRecord, RowToInsert> rows, BlobInfo blobInfo) {
    try {
      uploadBlobToGcs(toJson(rows.values()).getBytes("UTF-8"), blobInfo);
    } catch (UnsupportedEncodingException uee) {
      // Practically unreachable in modern JVMs, but we keep a clear domain exception
      throw new GcsConnectException("Failed to upload blob to GCS", uee);
    }
  }

  private void uploadBlobToGcs(byte[] blobContent, BlobInfo blobInfo) {
    storage.create(blobInfo, blobContent); // todo options: like a retention policy maybe?
  }

  /**
   * Converts a list of rows to a serialized JSON string of records.
   *
   * @param rows rows to be serialized
   * @return The resulting newline delimited JSON string containing all records in the original
   * list
   */
  private String toJson(Collection<RowToInsert> rows) {
    StringBuilder jsonRecordsBuilder = new StringBuilder("");
    for (RowToInsert row : rows) {
      Map<String, Object> record = row.getContent();
      jsonRecordsBuilder.append(gson.toJson(record));
      jsonRecordsBuilder.append("\n");
    }
    return jsonRecordsBuilder.toString();
  }

  /**
   * Execute the supplied function with retries and exponential backoff within a time budget. Ensure
   * at least one attempt even if timeout == 0; clamp sleeps to remaining time; use attempt-indexed
   * backoff capped by MAX_BACKOFF_MS; and respect the configured `retries`.
   *
   * @param func the operation to execute
   * @param timeout maximum time to keep retrying (sleep is clamped by remaining time)
   * @return result of the function, or {@code null} if timeout expires before a successful call
   */
  private <T> T executeWithRetry(Supplier<T> func, Duration timeout) throws InterruptedException {
    final long start = time.milliseconds(); // explicit clock to compute remaining
    final long budget = timeout == null ? 0L : Math.max(0L, timeout.toMillis());

    int attempt = 0; // number of retries already performed (sleep count)
    // Always make the first attempt, regardless of budget.
    while (true) {
      try {
        return func.get();
      } catch (BaseServiceException e) {
        if (!e.isRetryable()) {
          logger.error("Non-retryable exception on attempt {}", attempt + 1, e);
          throw e;
        }
        if (attempt >= retries) {
          // Out of configured retries
          logger.error("Operation failed after {} attempts (no retries left).", attempt + 1);
          throw e;
        }

        // Compute next backoff = min(MAX_BACKOFF_MS, retryWaitMs * 2^attempt)
        long base = Math.max(0L, retryWaitMs);
        long delay = computeBackoff(base, attempt, MAX_BACKOFF_MS);

        // Clamp to remaining time if a budget is provided
        if (budget > 0) {
          long elapsed = time.milliseconds() - start;
          long remaining = budget - elapsed;
          if (remaining <= 0) {
            logger.error("Timeout expired after {} attempts within {} ms budget.", attempt + 1, budget);
            return null;
          }
          delay = Math.min(delay, Math.max(0L, remaining));
        }

        // Add up to 1s jitter
        delay += (delay > 0 ? random.nextInt(WAIT_MAX_JITTER) : 0);

        logger.info(
            "Retryable exception on attempt {}: {}. Backing off {} ms",
            attempt + 1,
            e.getMessage(),
            delay
        );

        if (delay > 0) {
          time.sleep(delay);
        }
        attempt++;
      }
    }
  }

  /**
   * Computes the exponential backoff delay for a given retry attempt. The delay is calculated as:
   *
   * <pre>delay = baseDelayMs * (2 ^ attemptIndex)</pre>
   * but is clamped to an upper bound {@code capMs}. This ensures that the backoff grows
   * exponentially with each retry, but never exceeds the configured cap.
   *
   * <p>Examples (baseDelayMs = 100, capMs = 10_000):
   *
   * <ul>
   *   <li>attemptIndex = 0 → 100 ms
   *   <li>attemptIndex = 1 → 200 ms
   *   <li>attemptIndex = 2 → 400 ms
   *   <li>attemptIndex = 6 → 6,400 ms
   *   <li>attemptIndex = 7 → 10,000 ms (clamped)
   * </ul>
   *
   * @param baseDelayMs the initial delay in milliseconds (typically {@code retryWaitMs}); values ≤
   *     0 result in a delay of 0
   * @param attemptIndex zero-based retry attempt index (0 for the first retry after the initial
   *     attempt)
   * @param capMs maximum delay in milliseconds for any single backoff sleep
   * @return the computed delay in milliseconds, guaranteed to be between 0 and {@code capMs}
   */
  private static long computeBackoff(long baseDelayMs, int attemptIndex, long capMs) {
    if (baseDelayMs <= 0L) {
      return 0L;
    }
    long backoff = baseDelayMs;
    for (int i = 0; i < attemptIndex; i++) {
      if (backoff >= capMs) {
        return capMs;
      }
      long next = backoff << 1;
      if (next < 0) {
        return capMs; // overflow guard
      }
      backoff = next;
    }
    return Math.min(backoff, capMs);
  }

}
