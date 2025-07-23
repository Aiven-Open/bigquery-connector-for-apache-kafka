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

package com.wepay.kafka.connect.bigquery;

import static com.wepay.kafka.connect.bigquery.utils.TableNameUtils.intTable;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.batch.GcsBatchTableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.GcsToBqWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.UpsertDeleteBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiBase;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiBatchApplicationStream;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiWriter;
import io.aiven.kafka.utils.VersionInfo;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);
  private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 30;
  private final BigQuery testBigQuery;
  private final Storage testGcs;
  private final SchemaManager testSchemaManager;
  private final UUID uuid = UUID.randomUUID();
  private final StorageWriteApiBase testStorageWriteApi;
  private final StorageApiBatchModeHandler testStorageApiBatchHandler;
  private final Time time;
  @VisibleForTesting
  ScheduledExecutorService loadExecutor;
  private AtomicReference<BigQuery> bigQuery;
  private AtomicReference<SchemaManager> schemaManager;
  private SchemaRetriever schemaRetriever;
  private BigQueryWriter bigQueryWriter;
  private GcsToBqWriter gcsToBqWriter;
  private BigQuerySinkTaskConfig config;
  private SinkRecordConverter recordConverter;
  private RecordTableResolver recordTableResolver;
  private boolean upsertDelete;
  private MergeBatches mergeBatches;
  private MergeQueries mergeQueries;
  private volatile boolean stopped;
  private TopicPartitionManager topicPartitionManager;
  private KcbqThreadPoolExecutor executor;
  private int remainingRetries;
  private boolean enableRetries;
  private ErrantRecordHandler errantRecordHandler;
  private boolean useStorageApi;
  private boolean useStorageApiBatchMode;
  private StorageWriteApiBase storageApiWriter;
  private StorageApiBatchModeHandler batchHandler;
  private boolean autoCreateTables;
  private int retry;
  private long retryWait;
  private boolean allowNewBigQueryFields;
  private boolean allowRequiredFieldRelaxation;
  private boolean allowSchemaUnionization;

  /**
   * Create a new BigquerySinkTask.
   */
  public BigQuerySinkTask() {
    testBigQuery = null;
    schemaRetriever = null;
    testGcs = null;
    testSchemaManager = null;
    testStorageWriteApi = null;
    testStorageApiBatchHandler = null;
    time = Time.SYSTEM;
  }

  /**
   * For testing purposes only; will never be called by the Kafka Connect framework.
   *
   * @param testBigQuery      {@link BigQuery} to use for testing (likely a mock)
   * @param schemaRetriever   {@link SchemaRetriever} to use for testing (likely a mock)
   * @param testGcs           {@link Storage} to use for testing (likely a mock)
   * @param testSchemaManager {@link SchemaManager} to use for testing (likely a mock)
   * @param time              {@link Time} used to wait during backoff periods; should be mocked for testing
   * @see BigQuerySinkTask#BigQuerySinkTask()
   */
  public BigQuerySinkTask(BigQuery testBigQuery, SchemaRetriever schemaRetriever, Storage testGcs,
                          SchemaManager testSchemaManager, StorageWriteApiBase testStorageWriteApi,
                          StorageApiBatchModeHandler testStorageApiBatchHandler, Time time) {
    this.testBigQuery = testBigQuery;
    this.schemaRetriever = schemaRetriever;
    this.testGcs = testGcs;
    this.testSchemaManager = testSchemaManager;
    this.testStorageWriteApi = testStorageWriteApi;
    this.testStorageApiBatchHandler = testStorageApiBatchHandler;
    this.time = time;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (upsertDelete) {
      throw new ConnectException("This connector cannot perform upsert/delete on older versions of "
          + "the Connect framework; please upgrade to version 0.10.2.0 or later");
    } else if (useStorageApi && useStorageApiBatchMode) {
      throw new ConnectException("This connector cannot use batch mode for the Storage Write API "
          + "on older versions of the Connect framework; please upgrade to version 0.10.2.0 or later");
    }

    // Return immediately here since the executor will already be shutdown
    if (stopped) {
      // Still have to check for errors in order to prevent offsets being committed for records that
      // we've failed to write
      executor.maybeThrowEncounteredError();
      return;
    }

    try {
      executor.awaitCurrentTasks();
    } catch (InterruptedException err) {
      throw new ConnectException("Interrupted while waiting for write tasks to complete.", err);
    }

    topicPartitionManager.resumeAll();
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    if (upsertDelete) {
      Map<TopicPartition, OffsetAndMetadata> result = mergeBatches.latestOffsets();
      checkQueueSize();
      return result;
    } else if (useStorageApiBatchMode) {
      Map<TopicPartition, OffsetAndMetadata> result = batchHandler.getCommitableOffsets();
      logger.debug("Commitable Offsets for storage api batch mode : " + result.toString());
      return result;
    }

    flush(offsets);
    return offsets;
  }

  private void writeSinkRecords(Collection<SinkRecord> records) {
    // Periodically poll for errors here instead of doing a stop-the-world check in flush()
    maybeThrowErrors();

    logger.debug("Putting {} records in the sink.", records.size());

    // create tableWriters
    Map<PartitionedTableId, TableWriterBuilder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null || config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)) {
        PartitionedTableId table = recordTableResolver.getRecordTable(record);
        if (!tableWriterBuilders.containsKey(table)) {
          TableWriterBuilder tableWriterBuilder;
          if (useStorageApi) {
            tableWriterBuilder = new StorageWriteApiWriter.Builder(
                storageApiWriter,
                table,
                recordConverter,
                batchHandler
            );
          } else if (config.getList(BigQuerySinkConfig.ENABLE_BATCH_CONFIG).contains(record.topic())) {
            String topic = record.topic();
            long offset = record.kafkaOffset();
            String gcsBlobName = topic + "_" + uuid + "_" + Instant.now().toEpochMilli() + "_" + offset;
            String gcsFolderName = config.getString(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG);
            if (gcsFolderName != null && !"".equals(gcsFolderName)) {
              gcsBlobName = gcsFolderName + "/" + gcsBlobName;
            }
            tableWriterBuilder = new GcsBatchTableWriter.Builder(
                gcsToBqWriter,
                table.getBaseTableId(),
                config.getString(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG),
                gcsBlobName,
                recordConverter);
          } else {
            TableWriter.Builder simpleTableWriterBuilder =
                new TableWriter.Builder(bigQueryWriter, table, recordConverter);
            if (upsertDelete) {
              simpleTableWriterBuilder.onFinish(rows ->
                  mergeBatches.onRowWrites(table.getBaseTableId(), rows));
            }
            tableWriterBuilder = simpleTableWriterBuilder;
          }
          tableWriterBuilders.put(table, tableWriterBuilder);
        }
        try {
          tableWriterBuilders.get(table).addRow(record, table.getBaseTableId());
        } catch (ConversionConnectException ex) {
          // Send records to DLQ in case of ConversionConnectException
          if (errantRecordHandler.getErrantRecordReporter() != null) {
            errantRecordHandler.reportErrantRecords(Collections.singleton(record), ex);
          } else {
            throw ex;
          }
        }
      }
    }

    // add tableWriters to the executor work queue
    for (TableWriterBuilder builder : tableWriterBuilders.values()) {
      executor.execute(builder.build());
    }

    // check if we should pause topics
    checkQueueSize();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    try {
      writeSinkRecords(records);
      remainingRetries = config.getInt(BigQuerySinkConfig.MAX_RETRIES_CONFIG);
    } catch (RetriableException e) {
      if (enableRetries) {
        if (remainingRetries <= 0) {
          throw new ConnectException(e);
        } else {
          logger.warn("Write of records failed, remainingRetries={}", remainingRetries);
          remainingRetries--;
          throw e;
        }
      } else {
        throw e;
      }
    }
  }

  // Important: this method is only safe to call during put(), flush(), or preCommit(); otherwise,
  // a ConcurrentModificationException may be triggered if the Connect framework is in the middle of
  // a method invocation on the consumer for this task. This becomes especially likely if all topics
  // have been paused as the framework will most likely be in the middle of a poll for that consumer
  // which, because all of its topics have been paused, will not return until it's time for the next
  // offset commit. Invoking context.requestCommit() won't wake up the consumer in that case, so we
  // really have no choice but to wait for the framework to call a method on this task that implies
  // that it's safe to pause or resume partitions on the consumer.
  private void checkQueueSize() {
    long queueSoftLimit = config.getLong(BigQuerySinkConfig.QUEUE_SIZE_CONFIG);
    if (queueSoftLimit != -1) {
      int currentQueueSize = executor.getQueue().size();
      if (currentQueueSize > queueSoftLimit) {
        topicPartitionManager.pauseAll();
      } else if (currentQueueSize <= queueSoftLimit / 2) {
        // resume only if there is a reasonable chance we won't immediately have to pause again.
        topicPartitionManager.resumeAll();
      }
    }
  }

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    return bigQuery.updateAndGet(bq -> bq != null ? bq : newBigQuery());
  }

  private BigQuery newBigQuery() {
    return new GcpClientBuilder.BigQueryBuilder()
        .withConfig(config)
        .build();
  }

  private SchemaManager getSchemaManager() {
    if (testSchemaManager != null) {
      return testSchemaManager;
    }
    return schemaManager.updateAndGet(sm -> sm != null ? sm : newSchemaManager());
  }

  private SchemaManager newSchemaManager() {
    schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter =
        config.getSchemaConverter();
    Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
    Optional<String> kafkaDataFieldName = config.getKafkaDataFieldName();
    Optional<String> timestampPartitionFieldName = config.getTimestampPartitionFieldName();
    Optional<Long> partitionExpiration = config.getPartitionExpirationMs();
    Optional<List<String>> clusteringFieldName = config.getClusteringPartitionFieldNames();
    Optional<TimePartitioning.Type> timePartitioningType = config.getTimePartitioningType();
    boolean sanitizeFieldNames = config.getBoolean(BigQuerySinkConfig.SANITIZE_FIELD_NAME_CONFIG);
    return new SchemaManager(schemaRetriever, schemaConverter, getBigQuery(),
        allowNewBigQueryFields, allowRequiredFieldRelaxation, allowSchemaUnionization,
        sanitizeFieldNames,
        kafkaKeyFieldName, kafkaDataFieldName,
        timestampPartitionFieldName, partitionExpiration, clusteringFieldName, timePartitioningType);
  }

  private BigQueryWriter getBigQueryWriter(ErrantRecordHandler errantRecordHandler) {
    BigQuery bigQuery = getBigQuery();
    if (upsertDelete) {
      return new UpsertDeleteBigQueryWriter(bigQuery,
          getSchemaManager(),
          retry,
          retryWait,
          autoCreateTables,
          mergeBatches.intermediateToDestinationTables(),
          errantRecordHandler,
          time);
    } else if (autoCreateTables || allowNewBigQueryFields || allowRequiredFieldRelaxation) {
      return new AdaptiveBigQueryWriter(bigQuery,
          getSchemaManager(),
          retry,
          retryWait,
          autoCreateTables,
          errantRecordHandler,
          time);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait, errantRecordHandler, time);
    }
  }

  private Storage getGcs() {
    if (testGcs != null) {
      return testGcs;
    }
    return new GcpClientBuilder.GcsBuilder()
        .withConfig(config)
        .build();
  }

  private GcsToBqWriter getGcsWriter() {
    BigQuery bigQuery = getBigQuery();
    boolean attemptSchemaUpdate = allowNewBigQueryFields
        || allowRequiredFieldRelaxation
        || allowSchemaUnionization;
    // schemaManager shall only be needed for creating table or performing schema updates hence do
    // not fetch instance if not needed.
    boolean needsSchemaManager = autoCreateTables || attemptSchemaUpdate;
    SchemaManager schemaManager = needsSchemaManager ? getSchemaManager() : null;
    return new GcsToBqWriter(getGcs(),
        bigQuery,
        schemaManager,
        retry,
        retryWait,
        autoCreateTables,
        attemptSchemaUpdate,
        time);
  }

  private SinkRecordConverter getConverter(BigQuerySinkTaskConfig config) {
    return new SinkRecordConverter(config, mergeBatches, mergeQueries);
  }

  // visible for testing
  boolean isRunning() {
    return !stopped;
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    stopped = false;
    config = new BigQuerySinkTaskConfig(properties);
    autoCreateTables = config.getBoolean(BigQuerySinkConfig.TABLE_CREATE_CONFIG);
    upsertDelete = config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)
        || config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG);

    useStorageApi = config.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG);
    useStorageApiBatchMode = useStorageApi && config.getBoolean(BigQuerySinkConfig.ENABLE_BATCH_MODE_CONFIG);

    retry = config.getInt(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG);
    retryWait = config.getLong(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG);
    allowNewBigQueryFields = config.getBoolean(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG);
    allowRequiredFieldRelaxation = config.getBoolean(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG);
    allowSchemaUnionization = config.getBoolean(BigQuerySinkConfig.ALLOW_SCHEMA_UNIONIZATION_CONFIG);
    bigQuery = new AtomicReference<>();
    schemaManager = new AtomicReference<>();

    // Initialise errantRecordReporter
    ErrantRecordReporter errantRecordReporter = null;
    try {
      errantRecordReporter = context.errantRecordReporter(); // may be null if DLQ not enabled
    } catch (NoClassDefFoundError | NullPointerException e) {
      // Will occur in Connect runtimes earlier than 2.6
      logger.warn("Connect versions prior to Apache Kafka 2.6 do not support the errant record "
          + "reporter");
    }
    errantRecordHandler = new ErrantRecordHandler(errantRecordReporter);

    if (upsertDelete) {
      String intermediateTableSuffix = String.format("_%s_%d_%s_%d",
          config.getString(BigQuerySinkConfig.INTERMEDIATE_TABLE_SUFFIX_CONFIG),
          config.getInt(BigQuerySinkTaskConfig.TASK_ID_CONFIG),
          uuid,
          Instant.now().toEpochMilli()
      );
      mergeBatches = new MergeBatches(intermediateTableSuffix);
    }

    bigQueryWriter = getBigQueryWriter(errantRecordHandler);
    gcsToBqWriter = getGcsWriter();
    executor = new KcbqThreadPoolExecutor(
        config,
        new LinkedBlockingQueue<>(),
        new MdcContextThreadFactory()
    );
    topicPartitionManager = new TopicPartitionManager();
    recordTableResolver = new RecordTableResolver(config, mergeBatches, getBigQuery());

    if (config.getBoolean(BigQuerySinkTaskConfig.GCS_BQ_TASK_CONFIG)) {
      startGcsToBqLoadTask();
    } else if (upsertDelete) {
      mergeQueries =
          new MergeQueries(config, mergeBatches, executor, getBigQuery(), getSchemaManager(), context);
      maybeStartMergeFlushTask();
    } else if (useStorageApi) {
      initializeStorageApiMode();
    }

    recordConverter = getConverter(config);
    remainingRetries = config.getInt(BigQuerySinkConfig.MAX_RETRIES_CONFIG);
    enableRetries = config.getBoolean(BigQuerySinkConfig.ENABLE_RETRIES_CONFIG);
  }

  private void initializeStorageApiMode() {
    if (testStorageWriteApi != null) {
      logger.info("Starting task with Test Storage Write API Stream");
      storageApiWriter = testStorageWriteApi;
      batchHandler = testStorageApiBatchHandler;
      if (loadExecutor == null) {
        loadExecutor = Executors.newScheduledThreadPool(1, new MdcContextThreadFactory());
      }
      int commitInterval = config.getInt(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG);
      loadExecutor.scheduleAtFixedRate(this::batchLoadExecutorRunnable, commitInterval, commitInterval, TimeUnit.SECONDS);
    } else {
      boolean attemptSchemaUpdate = allowNewBigQueryFields || allowRequiredFieldRelaxation;
      BigQueryWriteSettings writeSettings = new GcpClientBuilder.BigQueryWriteSettingsBuilder().withConfig(config).build();
      if (useStorageApiBatchMode) {
        StorageWriteApiBatchApplicationStream writer = new StorageWriteApiBatchApplicationStream(
            retry,
            retryWait,
            writeSettings,
            autoCreateTables,
            errantRecordHandler,
            getSchemaManager(),
            attemptSchemaUpdate
        );
        storageApiWriter = writer;

        logger.info("Starting task with Storage Write API Batch Mode");
        batchHandler = new StorageApiBatchModeHandler(writer, config);

        int commitInterval = config.getInt(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG);
        logger.info("Starting Load Executor for Storage Write API Batch Mode with {} seconds interval ", commitInterval);
        loadExecutor = Executors.newScheduledThreadPool(1, new MdcContextThreadFactory());
        loadExecutor.scheduleAtFixedRate(this::batchLoadExecutorRunnable, commitInterval, commitInterval, TimeUnit.SECONDS);
      } else {
        logger.info("Starting task with Storage Write API Default Stream");
        storageApiWriter = new StorageWriteApiDefaultStream(
            retry,
            retryWait,
            writeSettings,
            autoCreateTables,
            errantRecordHandler,
            getSchemaManager(),
            attemptSchemaUpdate
        );
      }
    }
  }

  private void batchLoadExecutorRunnable() {
    try {
      batchHandler.refreshStreams();
    } catch (Throwable t) {
      logger.error("Storage Write API batch handler has failed due to : {}, {} ", t, Arrays.toString(t.getStackTrace()));
      loadExecutor.shutdown();
      logger.error("Shutting down the batch load handler");
    }
  }

  private void maybeThrowErrors() {
    executor.maybeThrowEncounteredError();
    if (useStorageApiBatchMode && loadExecutor.isTerminated()) {
      throw new BigQueryStorageWriteApiConnectException(
          "Batch load handler is terminated, failing task as no data would be written to bigquery tables!");
    }
  }

  private void startGcsToBqLoadTask() {
    logger.info("Attempting to start GCS Load Executor.");
    loadExecutor = Executors.newScheduledThreadPool(1, new MdcContextThreadFactory());
    String bucketName = config.getString(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG);
    Storage gcs = getGcs();
    // get the bucket, or create it if it does not exist.
    Bucket bucket = gcs.get(bucketName);
    if (bucket == null) {
      // todo here is where we /could/ set a retention policy for the bucket,
      // but for now I don't think we want to do that.
      if (config.getBoolean(BigQuerySinkConfig.AUTO_CREATE_BUCKET_CONFIG)) {
        BucketInfo bucketInfo = BucketInfo.of(bucketName);
        bucket = gcs.create(bucketInfo);
      } else {
        throw new ConnectException(String.format(
            "Bucket '%s' does not exist; Create the bucket manually, or set '%s' to true",
            bucketName,
            BigQuerySinkConfig.AUTO_CREATE_BUCKET_CONFIG
        ));
      }
    }
    GcsToBqLoadRunnable loadRunnable = new GcsToBqLoadRunnable(getBigQuery(), bucket);

    int intervalSec = config.getInt(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG);
    loadExecutor.scheduleAtFixedRate(loadRunnable, intervalSec, intervalSec, TimeUnit.SECONDS);
  }

  private void maybeStartMergeFlushTask() {
    long intervalMs = config.getLong(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG);
    if (intervalMs == -1) {
      logger.info("{} is set to -1; periodic merge flushes are disabled", BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG);
      return;
    }
    logger.info("Attempting to start upsert/delete load executor");
    loadExecutor = Executors.newScheduledThreadPool(1, new MdcContextThreadFactory());
    loadExecutor.scheduleAtFixedRate(
        mergeQueries::mergeFlushAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    try {
      maybeStopExecutor(loadExecutor, "load executor");
      maybeStopExecutor(executor, "table write executor");
      if (upsertDelete) {
        mergeBatches.intermediateTables().forEach(table -> {
          logger.debug("Deleting {}", intTable(table));
          getBigQuery().delete(table);
        });
      } else if (useStorageApi) {
        storageApiWriter.shutdown();
      }
    } finally {
      stopped = true;
    }

    logger.trace("task.stop()");
  }

  private void maybeStopExecutor(ExecutorService executor, String executorName) {
    if (executor == null) {
      return;
    }

    try {
      if (upsertDelete) {
        logger.trace("Forcibly shutting down {}", executorName);
        executor.shutdownNow();
      } else {
        logger.trace("Requesting shutdown for {}", executorName);
        executor.shutdown();
      }
      logger.trace("Awaiting termination of {}", executorName);
      executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
      logger.trace("Shut down {} successfully", executorName);
    } catch (Exception e) {
      logger.warn("Failed to shut down {}", executorName, e);
    }
  }

  @VisibleForTesting
  int getTaskThreadsActiveCount() {
    return executor.getActiveCount();
  }

  @Override
  public String version() {
    String version = new VersionInfo(BigQuerySinkTask.class).getVersion();
    logger.trace("task.version() = {}", version);
    return version;
  }

  private static class MdcContextThreadFactory implements ThreadFactory {

    private final Map<String, String> mdcContext;

    public MdcContextThreadFactory() {
      this.mdcContext = MDC.getCopyOfContextMap();
    }

    @Override
    public Thread newThread(Runnable runnable) {
      if (mdcContext == null) {
        return new Thread(runnable);
      } else {
        return new Thread(() -> {
          MDC.setContextMap(mdcContext);
          runnable.run();
        });
      }
    }
  }

  private class TopicPartitionManager {

    private Long lastChangeMs;
    private boolean isPaused;

    public TopicPartitionManager() {
      this.lastChangeMs = System.currentTimeMillis();
      this.isPaused = false;
    }

    public void pauseAll() {
      if (!isPaused) {
        long now = System.currentTimeMillis();
        logger.warn("Paused all partitions after {}ms", now - lastChangeMs);
        isPaused = true;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.pause(assignment.toArray(new TopicPartition[assignment.size()]));
    }

    public void resumeAll() {
      if (isPaused) {
        long now = System.currentTimeMillis();
        logger.info("Resumed all partitions after {}ms", now - lastChangeMs);
        isPaused = false;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.resume(assignment.toArray(new TopicPartition[assignment.size()]));
    }
  }
}
