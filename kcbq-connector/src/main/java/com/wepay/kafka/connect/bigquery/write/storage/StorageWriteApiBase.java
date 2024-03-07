package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
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
    do {
      try {
        List<ConvertedRecord> batch = batches.currentBatch();
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
        } else if (writeResult.hasError()) {
          Status errorStatus = writeResult.getError();
          String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, writeResult.getError().getMessage());
          retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage));
          if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(errorMessage)) {
            rows = maybeHandleDlqRoutingAndFilterRecords(rows, convertToMap(writeResult.getRowErrorsList()), tableName.getTable());
            if (rows.isEmpty()) {
              writer.onSuccess();
              return;
            }
          } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(errorStatus.getMessage())) {
            failTask(retryHandler.getMostRecentException());
          }
          logger.warn(errorMessage + " Retry attempt " + retryHandler.getAttempt());
        } else {
          if (!writeResult.hasAppendResult()) {
            logger.warn(
                "Write result did not report any errors, but also did not succeed. "
                    + "This may be indicative of a bug in the BigQuery Java client library or back end; "
                    + "please report it to the maintainers of the connector to investigate."
            );
          }
          logger.trace("Append call completed successfully on stream {}", streamName);
          writer.onSuccess();
          return;
        }
      } catch (BigQueryStorageWriteApiConnectException exception) {
        throw exception;
      } catch (Exception e) {
        String message = e.getMessage();
        String errorMessage = String.format("Failed to write rows on table %s due to %s", tableName, message);
        retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(errorMessage, e));

        if (shouldHandleSchemaMismatch(e)) {
          logger.warn("Sent records schema does not match with table schema, will attempt to update schema");
          retryHandler.attemptTableOperation(schemaManager::updateSchema);
        } else if (BigQueryStorageWriteApiErrorResponses.isMalformedRequest(message)) {
          rows = maybeHandleDlqRoutingAndFilterRecords(rows, getRowErrorMapping(e), tableName.getTable());
          if (rows.isEmpty()) {
            writer.onSuccess();
            return;
          }
        } else if (BigQueryStorageWriteApiErrorResponses.isStreamClosed(message)) {
          writer.refresh();
        } else if (BigQueryStorageWriteApiErrorResponses.isTableMissing(message) && getAutoCreateTables()) {
          retryHandler.attemptTableOperation(schemaManager::createTable);
        } else if (!BigQueryStorageWriteApiErrorResponses.isRetriableError(e.getMessage())
            && BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(e)
        ) {
          failTask(retryHandler.getMostRecentException());
        }
        logger.warn(errorMessage + " Retry attempt " + retryHandler.getAttempt());
      }
    } while (retryHandler.maybeRetry());
    throw new BigQueryStorageWriteApiConnectException(
        String.format("Exceeded %s attempts to write to table %s ", retryHandler.getAttempt(), tableName),
        retryHandler.getMostRecentException());
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
