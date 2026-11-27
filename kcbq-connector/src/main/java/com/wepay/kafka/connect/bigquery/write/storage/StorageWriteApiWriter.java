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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.strategy.CommitStrategy;
import com.wepay.kafka.connect.bigquery.strategy.StreamStateContainer;
import com.wepay.kafka.connect.bigquery.threads.ErrorRecorder;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Storage Write API writer that attempts to write all the rows it is given at once */
public final class StorageWriteApiWriter implements Runnable, StreamStateContainer {

  public static final String DEFAULT = "default";
  private final StorageWriteApiBase streamWriter;
  private final PartitionedTableId table;
  private final List<ConvertedRecord> records;
  private final String streamName;
  private final SinkRecordConverter recordConverter;
  private final Supplier<String> ulidSupplier;
  private final Logger logger = LoggerFactory.getLogger(StorageWriteApiWriter.class);
  private final StreamStateData streamStateData;
  private final ErrorRecorder errorRecorder;

  //  /**
  //   * @param tableName The table to write the records to
  //   * @param streamWriter The stream writer to use - Default, Batch etc
  //   * @param records The records to write
  //   * @param streamName The stream to use while writing data
  //   * @deprecated Use {@link #StorageWriteApiWriter(PartitionedTableId, StorageWriteApiBase,
  // List,
  //   *     String)} instead.
  //   */
  //  @Deprecated
  //  public StorageWriteApiWriter(
  //      TableName tableName,
  //      StorageWriteApiBase streamWriter,
  //      List<ConvertedRecord> records,
  //      String streamName) {
  //    this(TableNameUtils.partitionedTableId(tableName), streamWriter, records, streamName);
  //  }

  //  /**
  //   * @param table The table to write the records to
  //   * @param streamWriter The stream writer to use - Default, Batch etc
  //   * @param records The records to write
  //   * @param streamName The stream to use while writing data
  //   */
  //  public StorageWriteApiWriter(
  //      PartitionedTableId table,
  //      StorageWriteApiBase streamWriter,
  //      List<ConvertedRecord> records,
  //      String streamName) {
  //    this(table, streamWriter, records, streamName, null, null);
  //  }

  StorageWriteApiWriter(
      PartitionedTableId table,
      StorageWriteApiBase streamWriter,
      List<ConvertedRecord> records,
      String streamName,
      SinkRecordConverter recordConverter,
      Supplier<String> ulidSupplier,
      ErrorRecorder errorRecorder) {
    this.streamWriter = streamWriter;
    this.records = records;
    this.table = table;
    this.streamName = streamName;
    this.recordConverter = recordConverter;
    this.ulidSupplier = ulidSupplier;
    this.errorRecorder = errorRecorder;
    this.streamStateData = new StreamStateData(StreamState.CREATED);
  }

  @Override
  public void run() {
    if (streamStateData.isAborted()) {
      logger.debug("writing aborted");
      streamStateData.setStreamState(StreamState.FINALISED);
      return;
    }
    if (records.isEmpty()) {
      logger.debug("There are no records, skipping");
      streamStateData.setStreamState(StreamState.FINALISED);
      return;
    }
    try {
      this.streamStateData.setStreamState(StreamState.APPEND);
      logger.debug("Putting {} records into {} stream", records.size(), streamName);
      streamWriter.initializeAndWriteRecords(
          table, records, streamName, recordConverter, ulidSupplier);
      this.streamStateData.setStreamState(StreamState.COMMITTED);
    } catch (RuntimeException ex) {
      throw errorRecorder.logAndRecord(ex);
    }
  }

  @Override
  public String toString() {
    return "StorageWriteApi Writer " + streamName + " on " + table;
  }

  @Override
  public boolean stateIs(StreamState state) {
    return streamStateData.stateIs(state);
  }

  @Override
  public void abort() {
    streamStateData.abort();
  }

  public static class Builder implements TableWriterBuilder {
    private final List<ConvertedRecord> records = new ArrayList<>();
    private final SinkRecordConverter recordConverter;
    private final PartitionedTableId table;
    private final StorageWriteApiBase streamWriter;
    private final StorageApiBatchModeHandler batchModeHandler;
    private Supplier<String> ulidSupplier = null;
    private CommitStrategy commitStrategy;
    private ErrorRecorder errorRecorder;

    //    /**
    //     * @deprecated Use {@link #Builder(StorageWriteApiBase, PartitionedTableId,
    // SinkRecordConverter,
    //     *     StorageApiBatchModeHandler)} instead.
    //     */
    //    @Deprecated
    //    public Builder(
    //        StorageWriteApiBase streamWriter,
    //        TableName tableName,
    //        SinkRecordConverter recordConverter,
    //        StorageApiBatchModeHandler batchModeHandler) {
    //      this(
    //          streamWriter,
    //          TableNameUtils.partitionedTableId(tableName),
    //          recordConverter,
    //          batchModeHandler);
    //    }

    public Builder(
        StorageWriteApiBase streamWriter,
        PartitionedTableId table,
        SinkRecordConverter recordConverter,
        StorageApiBatchModeHandler batchModeHandler) {
      this.streamWriter = Objects.requireNonNull(streamWriter, "streamWriter may not be null");
      this.table = Objects.requireNonNull(table, "table may not be null");
      this.recordConverter =
          Objects.requireNonNull(recordConverter, "recordConverter may not be null");
      this.batchModeHandler =
          Objects.requireNonNull(batchModeHandler, "batchModeHandler may not be null");
    }

    /**
     * Enables write-attempt ID regeneration before every {@code appendRows()} call. When set, the
     * writer generates a fresh ULID from this supplier immediately before each gRPC write
     * (including the first attempt) and re-converts every row in the batch so that each actual
     * BigQuery API call carries a unique write-attempt ID.
     *
     * @param ulidSupplier supplier of ULID strings; must be thread-safe
     * @return this builder
     */
    public Builder withUlidSupplier(Supplier<String> ulidSupplier) {
      this.ulidSupplier = ulidSupplier;
      return this;
    }

    /**
     * Captures actual record and corresponding JSONObject converted record
     *
     * @param sinkRecord The actual records
     */
    @Override
    public void addRow(SinkRecord sinkRecord, TableId tableId) {
      records.add(new ConvertedRecord(sinkRecord, convertRecord(sinkRecord)));
    }

    @Override
    public void setCommitStrategy(CommitStrategy commitStrategy) {
      this.commitStrategy = commitStrategy;
    }

    @Override
    public void setErrorRecorder(ErrorRecorder errorRecorder) {
      this.errorRecorder = errorRecorder;
    }

    /**
     * Converts SinkRecord to JSONObject to be sent to BQ Streams
     *
     * @param record which is to be converted
     * @return converted record as JSONObject
     */
    private JSONObject convertRecord(SinkRecord record) {
      Map<String, Object> convertedRecord = recordConverter.getRow(record);
      return StorageWriteApiBase.getJsonFromMap(convertedRecord);
    }

    /**
     * @return Builds Storage write API writer which would do actual data ingestion using streams
     */
    @Override
    public Runnable build() {
      Objects.requireNonNull(errorRecorder, "errorRecorder may not be null");
      String streamName = DEFAULT;
      if (!records.isEmpty()
          && streamWriter instanceof StorageWriteApiBatchApplicationStream batchWriter) {
        TableName tableName = TableNameUtils.tableName(table.getBaseTableId());
        streamName = batchModeHandler.updateOffsetsOnStream(tableName.toString(), records);
      }
      StorageWriteApiWriter writer =
          new StorageWriteApiWriter(
              table,
              streamWriter,
              records,
              streamName,
              recordConverter,
              ulidSupplier,
              errorRecorder);
      if (DEFAULT.equals(streamName)) {
        Objects.requireNonNull(commitStrategy, "commitStrategy may not be null")
            .queue(records.stream().map(ConvertedRecord::original), writer);
      }
      return writer;
    }
  }
}
