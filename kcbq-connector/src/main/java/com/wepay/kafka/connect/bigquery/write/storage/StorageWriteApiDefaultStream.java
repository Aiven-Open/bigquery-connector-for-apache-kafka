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
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of {@link StorageWriteApiBase} which uses default streams to write data following at least once semantic
 */
public class StorageWriteApiDefaultStream extends StorageWriteApiBase {
  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiDefaultStream.class);
  ConcurrentMap<String, JsonStreamWriter> tableToStream = new ConcurrentHashMap<>();

  public StorageWriteApiDefaultStream(int retry,
                                      long retryWait,
                                      BigQueryWriteSettings writeSettings,
                                      boolean autoCreateTables,
                                      ErrantRecordHandler errantRecordHandler,
                                      SchemaManager schemaManager,
                                      boolean attemptSchemaUpdate,
                                      boolean ignoreUnknownFields) {
    super(
        retry,
        retryWait,
        writeSettings,
        autoCreateTables,
        errantRecordHandler,
        schemaManager,
        attemptSchemaUpdate,
        ignoreUnknownFields
    );
  }

  @Override
  public void preShutdown() {
    logger.info("Closing all writer for default stream on all tables");
    tableToStream.keySet().forEach(this::closeAndDelete);
    logger.info("Closed all writer for default stream on all tables");
  }

  /**
   * Either gets called when shutting down the task or when we receive exception that the stream
   * is actually closed on Google side. This will close and remove the stream from our cache.
   *
   * @param tableName The table name for which stream has to be removed.
   */
  private void closeAndDelete(String tableName) {
    tableToStream.computeIfPresent(tableName, (t, writer) -> {
      logger.debug("Closing stream on table {}", t);
      try {
        writer.close();
        logger.debug("Closed stream on table {}", t);
      } catch (Throwable e) {
        logger.warn("Error closing stream for table {}", t, e);
      }
      return null;
    });
  }

  /**
   * Open a default stream on table if not already present
   *
   * @param table The table on which stream has to be opened
   * @param rows  The input rows (would be sent while table creation to identify schema)
   * @return JSONStreamWriter which would be used to write data to bigquery table
   */
  @VisibleForTesting
  JsonStreamWriter getDefaultStream(TableName table, List<ConvertedRecord> rows) {
    String tableName = table.toString();
    return tableToStream.computeIfAbsent(tableName, t -> {
      StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(table, getSinkRecords(rows), retry, retryWait, time);
      do {
        try {
          return jsonWriterFactory.create(tableName);
        } catch (Exception e) {
          String baseErrorMessage = String.format(
              "Failed to create Default stream writer on table %s due to %s",
              tableName,
              e.getMessage());
          retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(baseErrorMessage, e));
          if (shouldHandleTableCreation(e.getMessage())) {
            retryHandler.attemptTableOperation(schemaManager::createTable);
          } else if (isNonRetriable(e)) {
            throw retryHandler.getMostRecentException();
          }
          logger.warn(baseErrorMessage + " Retry attempt {}", retryHandler.getAttempt());
        }
        retryHandler.maybeRetry("create default stream on table " + tableName);
      } while (true);
    });
  }

  @Override
  protected void updateJsonStreamWriterBuilder(JsonStreamWriter.Builder builder) {
    builder.setEnableConnectionPool(true);
  }

  @Override
  protected StreamWriter streamWriter(
      TableName tableName,
      String streamName,
      List<ConvertedRecord> records
  ) {
    return new DefaultStreamWriter(tableName, records);
  }

  class DefaultStreamWriter implements StreamWriter {

    private final TableName tableName;
    private final List<ConvertedRecord> inputRows;
    private JsonStreamWriter jsonStreamWriter;

    public DefaultStreamWriter(TableName tableName, List<ConvertedRecord> inputRows) {
      this.tableName = tableName;
      this.inputRows = inputRows;
    }

    @Override
    public ApiFuture<AppendRowsResponse> appendRows(
        JSONArray rows
    ) throws Descriptors.DescriptorValidationException, IOException {
      if (jsonStreamWriter == null) {
        jsonStreamWriter = getDefaultStream(tableName, inputRows);
      }
      return jsonStreamWriter.append(rows);
    }

    @Override
    public void onSuccess() {
      // no-op
    }

    @Override
    public void refresh() {
      closeAndDelete(tableName.toString());
      jsonStreamWriter = null;
    }

    @Override
    public String streamName() {
      return StorageWriteApiWriter.DEFAULT;
    }
  }

}
