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

package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at
 * least once semantic.
 *
 * <p>{@link JsonStreamWriter#append} is synchronized per instance and converts JSON to protobuf
 * inside the lock, so a single writer per table serializes every write thread targeting that table.
 * {@code defaultStreamWritersPerTable} opens up to N writers per table, each worker thread being
 * assigned one slot round-robin on its first write to any table and keeping it. Threads are spread
 * evenly over the slots when N equals {@code threadPoolSize}; smaller N is approximately balanced.
 * The default of 1 keeps the previous behavior.
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
  // Slots are created lazily; the atomic array publishes each writer to the lock-free fast path
  // in getDefaultStream.
  ConcurrentMap<String, AtomicReferenceArray<JsonStreamWriter>> tableToStreams =
      new ConcurrentHashMap<>();

  @VisibleForTesting int writersPerTable;
  @VisibleForTesting ThreadLocal<Integer> threadWriterSlot;

  public StorageWriteApiDefaultStream(
      int retry,
      long retryWait,
      BigQueryWriteSettings writeSettings,
      boolean autoCreateTables,
      ErrantRecordHandler errantRecordHandler,
      SchemaManager schemaManager,
      boolean attemptSchemaUpdate,
      BigQuerySinkConfig config) {
    super(
        retry,
        retryWait,
        writeSettings,
        autoCreateTables,
        errantRecordHandler,
        schemaManager,
        attemptSchemaUpdate,
        config);
    this.writersPerTable =
        config.getInt(BigQuerySinkConfig.DEFAULT_STREAM_WRITERS_PER_TABLE_CONFIG);
    this.threadWriterSlot = slotAssigner(writersPerTable);
  }

  /**
   * @deprecated This constructor does not support configuration of additional write settings. Use
   *     {@link #StorageWriteApiDefaultStream(int retry, long retryWait, BigQueryWriteSettings
   *     writeSettings, boolean autoCreateTables, ErrantRecordHandler errantRecordHandler,
   *     SchemaManager schemaManager, boolean attemptSchemaUpdate, BigQuerySinkConfig config)}
   *     instead.
   */
  @Deprecated
  public StorageWriteApiDefaultStream(
      int retry,
      long retryWait,
      BigQueryWriteSettings writeSettings,
      boolean autoCreateTables,
      ErrantRecordHandler errantRecordHandler,
      SchemaManager schemaManager,
      boolean attemptSchemaUpdate) {
    super(
        retry,
        retryWait,
        writeSettings,
        autoCreateTables,
        errantRecordHandler,
        schemaManager,
        attemptSchemaUpdate);
    this.writersPerTable = BigQuerySinkConfig.DEFAULT_STREAM_WRITERS_PER_TABLE_DEFAULT;
    this.threadWriterSlot = slotAssigner(writersPerTable);
  }

  /** Assigns each thread the next slot on first use, round-robin, and keeps it for that thread. */
  @VisibleForTesting
  static ThreadLocal<Integer> slotAssigner(int writersPerTable) {
    AtomicInteger next = new AtomicInteger(0);
    return ThreadLocal.withInitial(() -> Math.floorMod(next.getAndIncrement(), writersPerTable));
  }

  @Override
  public void preShutdown() {
    logger.info("Closing all writers for default stream on all tables");
    tableToStreams.keySet().forEach(this::closeAndDelete);
    logger.info("Closed all writers for default stream on all tables");
  }

  /**
   * Either gets called when shutting down the task or when we receive exception that the stream is
   * actually closed on Google side. This will close and remove every writer for the table.
   *
   * @param tableName The table name for which writers have to be removed.
   */
  private void closeAndDelete(String tableName) {
    tableToStreams.computeIfPresent(
        tableName,
        (t, writers) -> {
          logger.debug("Closing {} writer(s) on table {}", writers.length(), t);
          for (int i = 0; i < writers.length(); i++) {
            JsonStreamWriter writer = writers.get(i);
            if (writer == null) {
              continue;
            }
            try {
              writer.close();
              logger.debug("Closed writer {} on table {}", i, t);
            } catch (Throwable e) {
              logger.warn("Error closing writer {} for table {}", i, t, e);
            }
          }
          return null;
        });
  }

  /**
   * Close and remove one writer of a table, only if it is still the writer that failed. A writer
   * that another thread already replaced is left alone.
   */
  private void closeSlot(String tableName, int slot, JsonStreamWriter failed) {
    tableToStreams.computeIfPresent(
        tableName,
        (t, writers) -> {
          if (writers.compareAndSet(slot, failed, null)) {
            logger.debug("Closing writer {} on table {}", slot, t);
            try {
              failed.close();
            } catch (Throwable e) {
              logger.warn("Error closing writer {} for table {}", slot, t, e);
            }
          }
          return writers;
        });
  }

  /**
   * Open a default stream on table if not already present in the calling thread's slot.
   *
   * @param table The table on which stream has to be opened
   * @param rows The input rows (would be sent while table creation to identify schema)
   * @return JSONStreamWriter which would be used to write data to bigquery table
   */
  @VisibleForTesting
  JsonStreamWriter getDefaultStream(PartitionedTableId table, List<ConvertedRecord> rows) {
    String tableName = TableNameUtils.tableName(table.getFullTableId()).toString();
    int slot = writerSlot();
    AtomicReferenceArray<JsonStreamWriter> writers =
        tableToStreams.computeIfAbsent(tableName, t -> new AtomicReferenceArray<>(writersPerTable));
    JsonStreamWriter existing = writers.get(slot);
    if (existing != null) {
      return existing;
    }
    // The fast path above may return a writer that a concurrent close just retired; the append
    // then fails with a closed-stream error and the caller refreshes, as before this change.
    // Creation runs inside compute() so it serializes with close paths on the table key and a
    // concurrent close cannot drop the entry while a stream is still being opened.
    // Capture the writer inside compute(): a re-read of the slot after compute() returns could
    // observe a concurrent closeSlot() and yield null.
    AtomicReference<JsonStreamWriter> created = new AtomicReference<>();
    tableToStreams.compute(
        tableName,
        (t, arr) -> {
          if (arr == null) {
            arr = new AtomicReferenceArray<>(writersPerTable);
          }
          JsonStreamWriter writer = arr.get(slot);
          if (writer == null) {
            writer = createDefaultStream(table, tableName, rows);
            arr.set(slot, writer);
          }
          created.set(writer);
          return arr;
        });
    return created.get();
  }

  @VisibleForTesting
  JsonStreamWriter createDefaultStream(
      PartitionedTableId table, String tableName, List<ConvertedRecord> rows) {
    StorageWriteApiRetryHandler retryHandler =
        new StorageWriteApiRetryHandler(
            table.getBaseTableId(), getSinkRecords(rows), retry, retryWait, time);
    do {
      try {
        return jsonWriterFactory.create(tableName);
      } catch (Exception e) {
        String baseErrorMessage =
            String.format(
                "Failed to create Default stream writer on table %s due to %s",
                tableName, e.getMessage());
        retryHandler.setMostRecentException(
            new BigQueryStorageWriteApiConnectException(baseErrorMessage, e));
        if (shouldHandleTableCreation(e.getMessage())) {
          retryHandler.attemptTableOperation(schemaManager::createTable);
        } else if (isNonRetriable(e)) {
          throw retryHandler.getMostRecentException();
        }
        logger.warn(baseErrorMessage + " Retry attempt {}", retryHandler.getAttempt());
      }
      retryHandler.maybeRetry("create default stream on table " + tableName);
    } while (true);
  }

  /** Slot for the calling thread, assigned round-robin on first use and kept for its lifetime. */
  @VisibleForTesting
  int writerSlot() {
    return threadWriterSlot.get();
  }

  @Override
  protected void updateJsonStreamWriterBuilder(JsonStreamWriter.Builder builder) {
    builder.setEnableConnectionPool(true);
  }

  @Override
  protected StreamWriter streamWriter(
      PartitionedTableId table, String streamName, List<ConvertedRecord> records) {
    return new DefaultStreamWriter(table, records);
  }

  class DefaultStreamWriter implements StreamWriter {

    private final PartitionedTableId table;
    private final List<ConvertedRecord> inputRows;
    private JsonStreamWriter jsonStreamWriter;

    /**
     * @deprecated Use {@link #DefaultStreamWriter(PartitionedTableId, List)} instead.
     */
    @Deprecated
    public DefaultStreamWriter(TableName tableName, List<ConvertedRecord> inputRows) {
      this(TableNameUtils.partitionedTableId(tableName), inputRows);
    }

    public DefaultStreamWriter(PartitionedTableId table, List<ConvertedRecord> inputRows) {
      this.table = table;
      this.inputRows = inputRows;
    }

    @Override
    public ApiFuture<AppendRowsResponse> appendRows(JSONArray rows)
        throws Descriptors.DescriptorValidationException, IOException {
      if (jsonStreamWriter == null) {
        jsonStreamWriter = getDefaultStream(table, inputRows);
      }
      return jsonStreamWriter.append(rows);
    }

    @Override
    public void onSuccess() {
      // no-op
    }

    @Override
    public void refresh() {
      if (jsonStreamWriter != null) {
        closeSlot(
            TableNameUtils.tableName(table.getFullTableId()).toString(),
            writerSlot(),
            jsonStreamWriter);
      }
      jsonStreamWriter = null;
    }

    @Override
    public String streamName() {
      return StorageWriteApiWriter.DEFAULT;
    }
  }
}
