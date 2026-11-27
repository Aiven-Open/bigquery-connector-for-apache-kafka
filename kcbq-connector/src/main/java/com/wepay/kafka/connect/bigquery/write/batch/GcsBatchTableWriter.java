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

package com.wepay.kafka.connect.bigquery.write.batch;

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.strategy.CommitStrategy;
import com.wepay.kafka.connect.bigquery.strategy.StreamStateContainer;
import com.wepay.kafka.connect.bigquery.threads.ErrorRecorder;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.row.GcsToBqWriter;
import com.wepay.kafka.connect.bigquery.write.storage.StreamState;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch Table Writer that uploads records to GCS as a blob and then triggers a load job from that
 * GCS file to BigQuery.
 */
public class GcsBatchTableWriter implements Runnable, StreamStateContainer {
  private static final Logger logger = LoggerFactory.getLogger(GcsBatchTableWriter.class);

  private final TableId tableId;

  private final String bucketName;
  private final String blobName;

  private final SortedMap<SinkRecord, RowToInsert> rows;
  private final GcsToBqWriter writer;
  private final ErrantRecordHandler errantRecordHandler;
  private final ErrorRecorder errorRecorder;
  private final StreamStateData streamStateData;

  /**
   * @param rows The list of rows that should be written through GCS
   * @param writer {@link GcsToBqWriter} to use
   * @param tableId the BigQuery table id of the table to write to
   * @param bucketName the name of the GCS bucket where the blob should be uploaded
   * @param baseBlobName the base name of the blob in which the serialized rows should be uploaded.
   *     The full name is [baseBlobName]_[writerId]_
   */
  private GcsBatchTableWriter(
      SortedMap<SinkRecord, RowToInsert> rows,
      GcsToBqWriter writer,
      TableId tableId,
      String bucketName,
      String baseBlobName,
      ErrantRecordHandler errantRecordHandler,
      ErrorRecorder errorRecorder) {
    this.tableId = tableId;
    this.bucketName = bucketName;
    this.blobName = baseBlobName;
    this.rows = rows;
    this.writer = writer;
    this.errantRecordHandler = errantRecordHandler;
    this.errorRecorder = errorRecorder;
    this.streamStateData = new StreamStateData(StreamState.APPEND);
  }

  @Override
  public void run() {
    if (streamStateData.isAborted()) {
      streamStateData.setStreamState(StreamState.FINALISED);
      rows.clear();
      return;
    }
    try {
      writer.writeRows(rows, tableId, bucketName, blobName);
      streamStateData.setStreamState(StreamState.COMMITTED);
    } catch (RuntimeException | InterruptedException ex) {
      errantRecordHandler.reportErrantRecords(rows.keySet(), ex);
      errorRecorder.logAndRecord(new ConnectException("Failed to write rows to GCS", ex));
    }
  }

  @Override
  public boolean stateIs(StreamState state) {
    return streamStateData.stateIs(state);
  }

  @Override
  public void abort() {
    streamStateData.abort();
  }

  /** A Builder for constructing GCSBatchTableWriters. */
  public static class Builder implements TableWriterBuilder {
    private final String bucketName;
    private final TableId tableId;
    private final SortedMap<SinkRecord, RowToInsert> rows;
    private final SinkRecordConverter recordConverter;
    private final GcsToBqWriter writer;
    private final String blobName;
    private final ErrantRecordHandler errantRecordHandler;
    private CommitStrategy commitStrategy;
    private ErrorRecorder errorRecorder;

    /**
     * Create a {@link GcsBatchTableWriter.Builder}.
     *
     * @param writer the {@link GcsToBqWriter} to use.
     * @param tableId The bigquery table to be written to.
     * @param gcsBucketName The GCS bucket to write to.
     * @param gcsBlobName The name of the GCS blob to write.
     * @param recordConverter the {@link RecordConverter} to use.
     * @param errantRecordHandler the handler for records that can not be written.
     */
    public Builder(
        GcsToBqWriter writer,
        TableId tableId,
        String gcsBucketName,
        String gcsBlobName,
        SinkRecordConverter recordConverter,
        ErrantRecordHandler errantRecordHandler) {

      this.bucketName = gcsBucketName;
      this.blobName = gcsBlobName;
      this.tableId = tableId;

      this.rows =
          new TreeMap<>(
              Comparator.comparing(SinkRecord::kafkaPartition)
                  .thenComparing(SinkRecord::kafkaOffset));
      this.recordConverter = recordConverter;
      this.writer = writer;
      this.errantRecordHandler = errantRecordHandler;
      this.errorRecorder = ErrorRecorder.from(logger);
    }

    @Override
    public void addRow(SinkRecord record, TableId table) {
      rows.put(record, recordConverter.getRecordRow(record, table));
    }

    @Override
    public void setCommitStrategy(CommitStrategy commitStrategy) {
      this.commitStrategy = commitStrategy;
    }

    @Override
    public void setErrorRecorder(ErrorRecorder errorRecorder) {
      this.errorRecorder = errorRecorder;
    }

    @Override
    public GcsBatchTableWriter build() {
      Objects.requireNonNull(errorRecorder, "errorRecorder may not be null");
      Objects.requireNonNull(commitStrategy, "commitStrategy may not be null");
      return commitStrategy.queue(
          rows.keySet(),
          new GcsBatchTableWriter(
              rows, writer, tableId, bucketName, blobName, errantRecordHandler, errorRecorder));
    }
  }
}
