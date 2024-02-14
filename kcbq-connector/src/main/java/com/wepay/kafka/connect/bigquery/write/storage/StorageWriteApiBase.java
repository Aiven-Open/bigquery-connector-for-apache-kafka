package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.HashMap;

/**
 * Base class which handles data ingestion to bigquery tables using different kind of streams
 */
public abstract class StorageWriteApiBase {

    private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBase.class);
    private final ErrantRecordHandler errantRecordHandler;
    protected SchemaManager schemaManager;
    private BigQueryWriteClient writeClient;
    protected final int retry;
    protected final long retryWait;
    private final boolean autoCreateTables;
    private final BigQueryWriteSettings writeSettings;
    private final boolean attemptSchemaUpdate;

    /**
     * @param retry               How many retries to make in the event of a retriable error.
     * @param retryWait           How long to wait in between retries.
     * @param writeSettings       Write Settings for stream which carry authentication and other header information
     * @param autoCreateTables    boolean flag set if table should be created automatically
     * @param errantRecordHandler Used to handle errant records
     */
    public StorageWriteApiBase(int retry,
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
    }

    /**
     * Handles required initialization steps and goes to append records to table
     * @param tableName  The table to write data to
     * @param rows       List of pre- and post-conversion records.
     *                   Converted JSONObjects would be sent to api.
     *                   Pre-conversion sink records are required for DLQ routing
     * @param streamName The stream to use to write table to table.
     */
    public void initializeAndWriteRecords(TableName tableName, List<ConvertedRecord> rows, String streamName) {
        appendRows(tableName, rows, streamName);
    }

    abstract public void preShutdown();

    /**
     * Gets called on task.stop() and should have resource cleanup logic.
     */
    public void shutdown() {
        preShutdown();
        this.writeClient.close();
    }

    /**
     * @param tableName  The table to write data to
     * @param rows       The records to write
     * @param streamName The stream to use to write table to table.
     */
    abstract public void appendRows(TableName tableName, List<ConvertedRecord> rows, String streamName);

    /**
     * Creates Storage Api write client which carries all write settings information
     * @return Returns BigQueryWriteClient object
     * @throws IOException
     */
    public BigQueryWriteClient getWriteClient() throws IOException {
        if (this.writeClient == null) {
            this.writeClient = BigQueryWriteClient.create(writeSettings);
        }
        return this.writeClient;
    }

    /**
     * Verifies the exception object and returns row-wise error map
     * @param exception if the exception is not of expected type
     * @return Map of row index to error message detail
     */
    protected Map<Integer, String> getRowErrorMapping(Exception exception) {
        if(exception.getCause() instanceof Exceptions.AppendSerializtionError) {
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

    protected  boolean canAttemptSchemaUpdate() {
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

    protected ErrantRecordHandler getErrantRecordHandler() {
        return this.errantRecordHandler;
    }

    /**
     * Sends errant records to configured DLQ and returns remaining
     * @param input List of pre- and post-conversion records
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

        if (getErrantRecordHandler().getErrantRecordReporter() != null) {
            getErrantRecordHandler().sendRecordsToDLQ(recordsToDlq);
        }

        return filteredRecords;
    }

    /**
     * Converts Row Error to Map
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
        if (getErrantRecordHandler().getErrantRecordReporter() != null) {
            //Routes to DLQ
            return sendErrantRecordsToDlqAndFilterValidRecords(rows, errorMap);
        } else {
            // Fail if no DLQ
            logger.warn("DLQ is not configured!");
            throw new BigQueryStorageWriteApiConnectException(tableName, errorMap);
        }
    }
}

