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
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of {@link StorageWriteApiBase} which uses application streams for batch loading data
 * following at least once semantics.
 * Current/Active stream means - Stream which is not yet finalised and would be used for any new
 * data append.
 * Other streams (non-current/ non-active streams) - These streams may/may not be finalised yet but
 * would not be used for any new data. These will only write data for offsets assigned so far.
 */
public class StorageWriteApiBatchApplicationStream extends StorageWriteApiBase {

  private static final Logger logger = LoggerFactory.getLogger(StorageWriteApiBatchApplicationStream.class);

  /**
   * Map of {tableName , {StreamName, {@link ApplicationStream}}}
   * Streams should be accessed in the order of entry, so we need LinkedHashMap here
   */
  protected ConcurrentMap<String, LinkedHashMap<String, ApplicationStream>> streams;

  /**
   * Quick lookup for current open stream by tableName
   */
  protected ConcurrentMap<String, String> currentStreams;

  /**
   * Lock on table names to prevent execution of critical section by multiple threads
   */
  protected ConcurrentMap<String, Object> tableLocks;
  protected ConcurrentMap<ApplicationStream, Object> streamLocks;

  public StorageWriteApiBatchApplicationStream(
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
        config
    );
    streams = new ConcurrentHashMap<>();
    currentStreams = new ConcurrentHashMap<>();
    tableLocks = new ConcurrentHashMap<>();
    streamLocks = new ConcurrentHashMap<>();
  }

  /**
   * @deprecated This constructor does not support configuration of additional write settings.
   * Use {@link #StorageWriteApiBatchApplicationStream(int retry, long retryWait, BigQueryWriteSettings writeSettings,
   * boolean autoCreateTables, ErrantRecordHandler errantRecordHandler, SchemaManager schemaManager,
   * boolean attemptSchemaUpdate, BigQuerySinkConfig config)} instead.
   */
  @Deprecated
  public StorageWriteApiBatchApplicationStream(
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
    streams = new ConcurrentHashMap<>();
    currentStreams = new ConcurrentHashMap<>();
    tableLocks = new ConcurrentHashMap<>();
    streamLocks = new ConcurrentHashMap<>();
  }

  /**
   * Takes care of resource cleanup
   */
  @Override
  public void preShutdown() {
    logger.debug("Shutting down all streams on all tables as due to task shutdown!!!");
    this.streams.values()
        .stream().flatMap(item -> item.values().stream())
        .collect(Collectors.toList())
        .forEach(ApplicationStream::closeStream);
    logger.debug("Shutting completed for all streams on all tables!");
  }

  @Override
  protected StreamWriter streamWriter(
      TableName tableName,
      String streamName,
      List<ConvertedRecord> records
  ) {
    ApplicationStream applicationStream = this.streams.get(tableName.toString()).get(streamName);
    applicationStream.increaseAppendCall();
    return new BatchStreamWriter(applicationStream, tableName, streamName);
  }

  /**
   * Gets commitable offsets on all tables and all streams. Offsets returned should be sequential.
   * As soon as we see a stream not committed we will drop iterating over next streams for that
   * table. Cleans up committed streams
   *
   * @return Returns Map of TopicPartition to OffsetMetadata. Will be empty if there is nothing new
   *   to commit.
   */
  public Map<TopicPartition, OffsetAndMetadata> getCommitableOffsets() {
    Map<TopicPartition, OffsetAndMetadata> offsetsReadyForCommits = new ConcurrentHashMap<>();
    this.streams.forEach((tableName, streamDetails) -> {
          synchronized (lock(tableName)) {
            int i = 0;
            Set<String> deletableStreams = new HashSet<>();
            for (Map.Entry<String, ApplicationStream> applicationStreamEntry : streamDetails.entrySet()) {
              ApplicationStream applicationStream = applicationStreamEntry.getValue();
              String streamName = applicationStreamEntry.getKey();
              if (applicationStream.isInactive()) {
                logger.trace("Ignoring inactive stream {} at index {}", streamName, i);
              } else if (applicationStream.isReadyForOffsetCommit()) {
                logger.trace("Pulling offsets from committed stream {} at index {} ", streamName, i);
                offsetsReadyForCommits.putAll(applicationStream.getOffsetInformation());
                applicationStream.markInactive();
              } else {
                logger.trace("Ignoring all streams as stream {} at index {} is not yet committed", streamName, i);
                // We move sequentially for offset commit, until current offsets are ready, we cannot commit next.
                break;
              }
              deletableStreams.add(streamName);
              i++;
            }
            deletableStreams.forEach(streamDetails::remove);
          }
        }
    );

    logger.trace("Commitable offsets are {} for all tables on all eligible stream  : ", offsetsReadyForCommits);

    return offsetsReadyForCommits;
  }


  /**
   * This attempts to create stream if there are no existing stream for table or the stream is not empty
   * (it has been assigned some records)
   *
   * @param tableName Name of the table in project/dataset/table format
   */
  public boolean maybeCreateStream(String tableName, List<ConvertedRecord> rows) {
    String streamName = this.currentStreams.get(tableName);
    boolean shouldCreateNewStream = (streamName == null)
        || (this.streams.get(tableName).get(streamName) != null
            && this.streams.get(tableName).get(streamName).canTransitionToNonActive());
    if (shouldCreateNewStream) {
      logger.trace("Attempting to create new stream on table {}", tableName);
      return this.createStream(tableName, streamName, rows);
    }
    return false;
  }

  /**
   * Attempts to commit all eligible streams. A stream is eligible if it
   * {@link ApplicationStream#canBeCommitted can be committed} and
   * if it is no longer capable of receiving new records.
   *
   * <p>In addition, every stream that has received at least one record is
   * replaced by a new current stream.
   */
  public void refreshStreams() {
    // Normally, iterating over a concurrent collection is unsafe since there
    // is no guarantee that the collection won't be modified during iteration
    // However, the weak consistency guarantees provided by this kind of iterator
    // (see https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/package-summary.html#Weakly)
    // are sufficient: we are guaranteed to iterate once each over the elements in
    // the collection that existed at the start of iteration, and may or may not also
    // iterate over elements that were added later on
    // If an element (i.e., application stream) is added later, we'll just try to commit it
    // later, possibly in the next invocation of this method
    currentStreams.keySet().forEach(table -> maybeCreateStream(table, null));
  }

  /**
   * Assigns offsets to current stream on table
   *
   * @param tableName The name of table
   * @param rows      Offsets which are to be written by current stream to bigquery table
   * @return Stream name using which offsets would be written
   */
  public String updateOffsetsOnStream(
      String tableName,
      List<ConvertedRecord> rows
  ) {
    String streamName;
    Map<TopicPartition, OffsetAndMetadata> offsetInfo = getOffsetFromRecords(rows);
    synchronized (lock(tableName)) {
      streamName = this.getCurrentStreamForTable(tableName, rows);
      this.streams.get(tableName).get(streamName).updateOffsetInformation(offsetInfo, rows.size());
    }
    logger.trace("Assigned offsets {} to stream {} for {} rows", offsetInfo, streamName, rows.size());
    return streamName;
  }

  /**
   * Takes care of creating a new application stream
   */
  @VisibleForTesting
  ApplicationStream createApplicationStream(String tableName, List<ConvertedRecord> rows) {
    StorageWriteApiRetryHandler retryHandler = new StorageWriteApiRetryHandler(
        TableName.parse(tableName), rows != null ? getSinkRecords(rows) : null, retry, retryWait, time);
    do {
      try {
        return new ApplicationStream(tableName, getWriteClient(), jsonWriterFactory);
      } catch (Exception e) {
        String baseErrorMessage = String.format(
            "Failed to create Application stream writer on table %s due to %s",
            tableName,
            e.getMessage());
        retryHandler.setMostRecentException(new BigQueryStorageWriteApiConnectException(baseErrorMessage, e));
        if (shouldHandleTableCreation(e.getMessage())) {
          if (rows == null) {
            // We reached here as application stream creation is triggered by the scheduler and the
            // table does not exist. We do not have records to define the table schema so table creation
            // attempt cannot be made. Now we will rely on StorageWriteApiWriter to create table and
            // application stream
            return null;
          }
          retryHandler.attemptTableOperation(schemaManager::createTable);
        } else if (isNonRetriable(e)) {
          failTask(retryHandler.getMostRecentException());
        }
        logger.warn(baseErrorMessage + " Retry attempt {}", retryHandler.getAttempt());
      }
      retryHandler.maybeRetry("create application stream on table " + tableName);
    } while (true);
  }

  /**
   * Creates stream and updates current active stream with the newly created one
   *
   * @param tableName The name of the table
   * @param oldStream Last active stream on the table when this method was invoked.
   * @return Returns false if the oldstream is not equal to active stream , creates stream otherwise and returns true
   */
  private boolean createStream(String tableName, String oldStream, List<ConvertedRecord> rows) {
    synchronized (lock(tableName)) {
      // This check verifies if the current active stream is same as seen by the calling method. If different, that
      // would mean a new stream got created by some other thread and this attempt can be dropped.
      if (!Objects.equals(oldStream, this.currentStreams.get(tableName))) {
        return false;
      }
      // Current state is same as calling state. Create new Stream
      ApplicationStream stream = createApplicationStream(tableName, rows);
      if (stream == null) {
        if (rows == null) {
          return false;
        } else {
          // We should never reach here
          throw new BigQueryStorageWriteApiConnectException(
              "Application Stream creation could not be completed successfully.");
        }

      }
      String streamName = stream.getStreamName();

      this.streams.computeIfAbsent(tableName, t -> new LinkedHashMap<>());
      this.streams.get(tableName).put(streamName, stream);
      this.currentStreams.put(tableName, streamName);
    }
    if (oldStream != null) {
      commitStreamIfEligible(tableName, oldStream);
    }

    return true;
  }

  /**
   * This takes care of actually making the data available for viewing in BigQuery
   *
   * @param stream The stream which should be committed
   */
  private void finaliseAndCommitStream(ApplicationStream stream) {
    stream.finalise();
    stream.commit();
  }

  /**
   * Get or create stream for table
   *
   * @param tableName The table name
   * @return Current active stream on table
   */
  private String getCurrentStreamForTable(String tableName, List<ConvertedRecord> rows) {
    if (!currentStreams.containsKey(tableName)) {
      this.createStream(tableName, null, rows);
    }

    return Objects.requireNonNull(this.currentStreams.get(tableName));
  }

  /**
   * Commits the stream if it is not active and has written all the data assigned to it.
   *
   * @param tableName  The name of the table
   * @param streamName The name of the stream on table
   */
  private void commitStreamIfEligible(String tableName, String streamName) {
    if (!Objects.equals(currentStreams.get(tableName), streamName)) {
      logger.trace("Stream {} is not active, can be committed", streamName);
      ApplicationStream stream = this.streams.get(tableName).get(streamName);
      synchronized (lock(stream)) {
        if (stream != null && stream.areAllExpectedCallsCompleted()) {
          if (!stream.canBeCommitted()) {
            logger.trace("Stream {} with state {} is not committable", streamName, stream.getCurrentState());
            return;
          }
          // We are done with all expected calls for non-active streams, lets finalise and commit the stream.
          logger.trace("Stream {} has written all assigned offsets.", streamName);
          finaliseAndCommitStream(stream);
          logger.trace("Stream {} is now committed.", streamName);
          return;
        }
      }
      logger.trace("Stream {} has not written all assigned offsets.", streamName);
    }
    logger.trace("Stream {} on table {} is not eligible for commit yet", streamName, tableName);
  }

  private void updateSuccessAndTryCommit(ApplicationStream applicationStream, TableName tableName, String streamName) {
    applicationStream.increaseCompletedCalls();
    commitStreamIfEligible(tableName.toString(), streamName);
  }

  private Object lock(String tableName) {
    return tableLocks.computeIfAbsent(tableName, t -> new Object());
  }

  private Object lock(ApplicationStream stream) {
    return streamLocks.computeIfAbsent(stream, s -> new Object());
  }

  /**
   * This returns offset information of records
   *
   * @param records List of pre- and post-conversion records
   * @return Offsets of the SinkRecords in records list
   */
  private Map<TopicPartition, OffsetAndMetadata> getOffsetFromRecords(List<ConvertedRecord> records) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    records.forEach(record -> {
      SinkRecord sr = record.original();
      offsets.put(new TopicPartition(sr.topic(), sr.kafkaPartition()), new OffsetAndMetadata(sr.kafkaOffset() + 1));
    });

    return offsets;
  }

  class BatchStreamWriter implements StreamWriter {

    private final ApplicationStream applicationStream;
    private final TableName tableName;
    private final String streamName;

    public BatchStreamWriter(ApplicationStream applicationStream, TableName tableName, String streamName) {
      this.applicationStream = applicationStream;
      this.tableName = tableName;
      this.streamName = streamName;
    }

    @Override
    public ApiFuture<AppendRowsResponse> appendRows(
        JSONArray rows
    ) throws Descriptors.DescriptorValidationException, IOException {
      return applicationStream.writer().append(rows);
    }

    @Override
    public void onSuccess() {
      updateSuccessAndTryCommit(applicationStream, tableName, streamName);
    }

    @Override
    public void refresh() {
      // No-op; handled internally by ApplicationStream class
    }

    @Override
    public String streamName() {
      return streamName;
    }

  }

}
