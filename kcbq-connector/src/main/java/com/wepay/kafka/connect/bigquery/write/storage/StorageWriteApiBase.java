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
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Status;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiErrorResponses;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.RecordBatches;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {

  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
  protected final JsonStreamWriterFactory jsonWriterFactory;
  protected final int retry;
  protected final long retryWait;
  private final boolean autoCreateTables;
  private final BigQueryWriteSettings writeSettings;
  private final boolean attemptSchemaUpdate;
  protected SchemaManager schemaManager;
  @VisibleForTesting
  protected Time time;
  ErrantRecordHandler errantRecordHandler;
  private BigQueryWriteClient writeClient;

  /**
   * @param retry               How many retries to make in the event of a retriable error.
   * @param retryWait           How long to wait in between retries.
   * @param writeSettings       Write Settings for stream which carry authentication and other header information
   * @param autoCreateTables    boolean flag set if table should be created automatically
   * @param errantRecordHandler Used to handle errant records
   */
  protected StorageWriteApiBase(int retry,
                                long retryWait,
                                BigQueryWriteSettings writeSettings,
                                boolean autoCreateTables,
                                ErrantRecordHandler errantRecordHandler,
                                SchemaManager schemaManager,
                                boolean attemptSchemaUpdate) {
    this.retry = retry;
    this.retryWait = retryWait;
    this.autoCreateTables = autoCreateTables;
    this.writeSettings = writeSettings;
    this.errantRecordHandler = errantRecordHandler;
    this.schemaManager = schemaManager;
    this.attemptSchemaUpdate = attemptSchemaUpdate;
    try {
      this.writeClient = getWriteClient();
    } catch (IOException e) {
      logger.error("Failed to create Big Query Storage Write API write client due to {}", e.getMessage());
      throw new BigQueryStorageWriteApiConnectException("Failed to create Big Query Storage Write API write client", e);
    }
    this.jsonWriterFactory = getJsonWriterFactory();
    this.time = Time.SYSTEM;
  }

  public abstract void preShutdown();

  protected abstract StreamWriter streamWriter(
      TableName tableName,
      String streamName,
      List<ConvertedRecord> records
  );

  /**
   * Gets called on task.stop() and should have resource cleanup logic.
   */
  public void shutdown() {
    preShutdown();
    this.writeClient.close();
  }

  /**
   * Handles required initialization steps and goes to append records to table
   *
   * @param tableName  The table to write data to
   * @param rows       List of pre- and post-conversion records.
   *                   Converted JSONObjects would be sent to api.
   *                   Pre-conversion sink records are required for DLQ routing
   * @param streamName The stream to use to write table to table.
   */
  public void initializeAndWriteRecords(TableName tableName, List<ConvertedRecord> rows, String streamName) {
    StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(tableName, getSinkRecords(rows), retry, retryWait, time);
    logger.debug("Sending {} records to write Api Application stream {}", rows.size(), streamName);
    RecordBatches<ConvertedRecord> batches = new RecordBatches<>(rows);
    StreamWriter writer = streamWriter(tableName, streamName, rows);
    while (!batches.completed()) {
      List<ConvertedRecord> batch = batches.currentBatch();

      while (!batch.isEmpty()) {
        try {
          writeBatch(writer, batch, retryHandler, tableName);
          batch = Collections.emptyList(); // Can't do batch.clear(); it'll mess with the batch tracking logic in RecordBatches
        } catch (RetryException e) {
          retryHandler.maybeRetry("write to table " + tableName);
          if (e.getMessage() != null) {
            logger.warn(e.getMessage() + " Retry attempt " + retryHandler.getAttempt());
          }
        } catch (BatchTooLargeException e) {
          if (batch.size() <= 1) {
            Map<Integer, String> rowErrorMapping = Collections.singletonMap(
                0, e.getMessage()
            );
            batch = maybeHandleDlqRoutingAndFilterRecords(batch, rowErrorMapping, tableName.getTable());
            if (!batch.isEmpty()) {
              retryHandler.maybeRetry("write to table " + tableName);
            }
          } else {
            int previousSize = batch.size();
            batches.reduceBatchSize();
            batch = batches.currentBatch();
            logger.debug("Reducing batch size for table {} from {} to {}", tableName, previousSize, batch.size());
          }
        } catch (MalformedRowsException e) {
          batch = maybeHandleDlqRoutingAndFilterRecords(batch, e.getRowErrorMapping(), tableName.getTable());
          if (!batch.isEmpty()) {
            // TODO: Does this actually make sense? Should we count this as part of our retry logic?
            //       As long as we're guaranteed that the number of rows in the batch is decreasing, it
            //       may make sense to skip the maybeRetry invocation
            retryHandler.maybeRetry("write to table " + tableName);
          }
        }
      }

      batches.advanceToNextBatch();
    }

    writer.onSuccess();
  }

  private void writeBatch(
      StreamWriter writer,
      List<ConvertedRecord> batch,
      StorageWriteApiRetryHandler retryHandler,
      TableName tableName
  ) throws BatchTooLargeException, MalformedRowsException, RetryException {
    try {
      JSONArray jsonRecords = getJsonRecords(batch);
      logger.trace("Sending records to Storage API writer for batch load");
      ApiFuture<AppendRowsResponse> response = writer.appendRows(jsonRecords);
      AppendRowsResponse writeResult = response.get();
      logger.trace("Received response from Storage API writer batch");

      if (writeResult.hasUpdatedSchema()) {
        logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
        if (!canAttemptSchemaUpdate()) {
          throw new BigQueryStorageWriteApiConnectException("Connector is not configured to perform schema updates.");
        }
        retryHandler.attemptTableOperation(schemaManager::updateSchema);
        throw new RetryException();
      } else if (writeResult.hasError()) {
        Status errorStatus = writeResult.getError();
        String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, writeResult.getError().getMessage());
        retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage));
        if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorMessage)) {
          throw new MalformedRowsException(convertToMap(writeResult.getRowErrorsList()));
        } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
          failTask(retryHandler.getMostRecentException());
        }
        throw new RetryException(errorMessage);
      } else {
        if (!writeResult.hasAppendResult()) {
          logger.warn(
              "Write result did not report any errors, but also did not succeed. "
                  + "This may be indicative of a bug in the BigQuery Java client library or back end; "
                  + "please report it to the maintainers of the connector to investigate."
          );
        }
        logger.trace("Append call completed successfully on stream {}", writer.streamName());
      }
    } catch (BigQueryStorageWriteApiConnectException | BatchWriteException exception) {
      throw exception;
    } catch (Exception e) {
      String message = e.getMessage();
      String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, message);
      retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage, e));

      if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(message)) {
        writer.refresh();
      } else if (shouldHandleSchemaMismatch(e)) {
        logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
        retryHandler.attemptTableOperation(schemaManager::updateSchema);
      } else if (BigQueryStorageWriteApiErrorResponses.isMessageTooLargeError(message)) {
        throw new BatchTooLargeException(errorMessage);
      } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
        throw new MalformedRowsException(getRowErrorMapping(e));
      } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && getAutoCreateTables()) {
        retryHandler.attemptTableOperation(schemaManager::createTable);
      } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())
          && BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(e)
      ) {
        failTask(retryHandler.getMostRecentException());
      }
      throw new RetryException(errorMessage);
    }
  }

  private abstract static class BatchWriteException extends Exception {

    protected BatchWriteException() {
      super();
    }

    protected BatchWriteException(String message) {
      super(message);
    }

  }

  private static class BatchTooLargeException extends BatchWriteException {

    public BatchTooLargeException(String message) {
      super(message);
    }

  }

  private static class MalformedRowsException extends BatchWriteException {

    private final Map<Integer, String> rowErrorMapping;

    public MalformedRowsException(Map<Integer, String> rowErrorMapping) {
      this.rowErrorMapping = rowErrorMapping;
    }

    public Map<Integer, String> getRowErrorMapping() {
      return rowErrorMapping;
    }

  }

  private static class RetryException extends BatchWriteException {

    public RetryException() {
      super();
    }

    public RetryException(String message) {
      super(message);
    }
  }

  /**
   * Creates Storage Api write client which carries all write settings information
   *
   * @return Returns BigQueryWriteClient object
   * @throws IOException
   */
  protected BigQueryWriteClient getWriteClient() throws IOException {
    if (this.writeClient == null) {
      this.writeClient = BigQueryWriteClient.create(writeSettings);
    }
    return this.writeClient;
  }

  /**
   * Returns a {@link JsonStreamWriterFactory} for creating configured {@link JsonStreamWriter} instances
   *
   * @return a {@link JsonStreamWriterFactory}
   */
  protected JsonStreamWriterFactory getJsonWriterFactory() {
    return streamOrTableName -> JsonStreamWriter.newBuilder(streamOrTableName, writeClient)
            .build();
  }

  /**
   * Verifies the exception object and returns row-wise error map
   *
   * @param exception if the exception is not of expected type
   * @return Map of row index to error message detail
   */
  protected Map<Integer, String> getRowErrorMapping(Exception exception) {
    if (exception.getCause() instanceof Exceptions.AppendSerializtionError) {
      exception = (Exceptions.AppendSerializtionError) exception.getCause();
    }
    if (exception instanceof Exceptions.AppendSerializtionError) {
      return ((Exceptions.AppendSerializtionError) exception).getRowIndexToErrorMessage();
    } else {
      throw new BigQueryStorageWriteApiConnectException(
          "Exception is not an instance of Exceptions.AppendSerializtionError", exception);
    }
  }

  protected boolean getAutoCreateTables() {
    return this.autoCreateTables;
  }

  protected boolean canAttemptSchemaUpdate() {
    return this.attemptSchemaUpdate;
  }

  /**
   * @param rows List of pre- and post-conversion records
   * @return Returns list of all pre-conversion records
   */
  protected List<SinkRecord> getSinkRecords(List<ConvertedRecord> rows) {
    return rows.stream()
        .map(ConvertedRecord::original)
        .collect(Collectors.toList());
  }

  /**
   * Sends errant records to configured DLQ and returns remaining
   *
   * @param input           List of pre- and post-conversion records
   * @param indexToErrorMap Map of record index to error received from api call
   * @return Returns list of good records filtered from input which needs to be retried. Append row does
   * not write partially even if there is a single failure, good data has to be retried
   */
  protected List<ConvertedRecord> sendErrantRecordsToDlqAndFilterValidRecords(
      List<ConvertedRecord> input,
      Map<Integer, String> indexToErrorMap) {
    List<ConvertedRecord> filteredRecords = new ArrayList<>();
    Map<SinkRecord, Throwable> recordsToDlq = new LinkedHashMap<>();

    for (int i = 0; i < input.size(); i++) {
      if (indexToErrorMap.containsKey(i)) {
        SinkRecord inputRecord = input.get(i).original();
        Throwable error = new Throwable(indexToErrorMap.get(i));
        recordsToDlq.put(inputRecord, error);
      } else {
        filteredRecords.add(input.get(i));
      }
    }

    if (errantRecordHandler.getErrantRecordReporter() != null) {
      errantRecordHandler.reportErrantRecords(recordsToDlq);
    }

    return filteredRecords;
  }

  /**
   * Converts Row Error to Map
   *
   * @param rowErrors List of row errors
   * @return Returns Map with key as Row index and value as the Row Error Message
   */
  protected Map<Integer, String> convertToMap(List<RowError> rowErrors) {
    Map<Integer, String> errorMap = new HashMap<>();

    rowErrors.forEach(rowError -> errorMap.put((int) rowError.getIndex(), rowError.getMessage()));

    return errorMap;
  }

  protected List<ConvertedRecord> maybeHandleDlqRoutingAndFilterRecords(
      List<ConvertedRecord> rows,
      Map<Integer, String> errorMap,
      String tableName
  ) {
    if (errantRecordHandler.getErrantRecordReporter() != null) {
      //Routes to DLQ
      return sendErrantRecordsToDlqAndFilterValidRecords(rows, errorMap);
    } else {
      // Fail if no DLQ
      logger.warn("DLQ is not configured!");
      throw new BigQueryStorageWriteApiConnectException(tableName, errorMap);
    }
  }

  private JSONArray getJsonRecords(List<ConvertedRecord> rows) {
    JSONArray jsonRecords = new JSONArray();
    for (ConvertedRecord item : rows) {
      jsonRecords.put(item.converted());
    }
    return jsonRecords;
  }

  protected boolean shouldHandleSchemaMismatch(Exception e) {
    if (!canAttemptSchemaUpdate()) {
      return false;
    }

    if (BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(Collections.singletonList(e.getMessage()))) {
      return true;
    }

    if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e.getMessage())
        && BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(getRowErrorMapping(e).values())) {
      return true;
    }

    return false;
  }

  protected boolean shouldHandleTableCreation(String errorMessage) {
    return BigQueryStorageWriteApiErrorResponses.isTableMissing(errorMessage) && getAutoCreateTables();
  }

  protected boolean isNonRetriable(Exception e) {
    return !BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())
        && BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(e);
  }

  protected void failTask(RuntimeException failure) {
    // Fail on non-retriable error
    logger.error("Encountered unrecoverable failure", failure);
    throw failure;
  }

}
