/*
 * Copyright 2022-2026 Aiven Oy and
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
 * https://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.AvroLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import com.wepay.kafka.connect.bigquery.strategy.CommitStrategy;
import com.wepay.kafka.connect.bigquery.strategy.DefaultKafkaCommitStrategy;
import com.wepay.kafka.connect.bigquery.strategy.HighestUnblockedCommitStrategy;
import com.wepay.kafka.connect.bigquery.strategy.StreamStateContainer;
import com.wepay.kafka.connect.bigquery.threads.KcbqScheduledThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.threads.KcbqThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.threads.NamedRunnable;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.NeedsStreamState;
import com.wepay.kafka.connect.bigquery.write.batch.GcsBatchTableWriter;
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
import com.wepay.kafka.connect.bigquery.write.storage.StreamState;
import de.huxhorn.sulky.ulid.ULID;
import io.aiven.commons.util.system.VersionInfo;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
  private Supplier<BigQuery> bigQuerySupplier;
  private Supplier<Storage> storageSupplier;
  private Supplier<SchemaManager> schemaManagerSupplier;
  private static final ULID ULID_GENERATOR = new ULID();
  private final String uuid = ULID_GENERATOR.nextULID();
  private final StorageWriteApiBase testStorageWriteApi;
  private final StorageApiBatchModeHandler testStorageApiBatchHandler;
  private final Time time;
  @VisibleForTesting KcbqScheduledThreadPoolExecutor scheduledExecutor;
  private final AtomicReference<BigQuery> bigQuery = new AtomicReference<>();
  private final AtomicReference<SchemaManager> schemaManager = new AtomicReference<>();
  private BigQueryWriter bigQueryWriter;
  private GcsToBqWriter gcsToBqWriter;
  private BigQuerySinkTaskConfig config;
  private SinkRecordConverter recordConverter;
  private RecordTableResolver recordTableResolver;
  private boolean upsertDelete;
  private MergeBatches mergeBatches;
  private MergeQueries mergeQueries;
  private volatile boolean stopped = true;
  private TopicPartitionManager topicPartitionManager;
  @VisibleForTesting KcbqThreadPoolExecutor executor;
  private int remainingRetries;
  private boolean enableRetries;
  private boolean trackPutAttempts;
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
  private CommitStrategy commitStrategy;
  private final Function<BigQuerySinkTaskConfig, CommitStrategy> commitStrategyFactory;

  /** Create a new BigquerySinkTask. */
  public BigQuerySinkTask() {
    bigQuerySupplier = null;
    storageSupplier = null;
    schemaManagerSupplier = null;
    testStorageWriteApi = null;
    testStorageApiBatchHandler = null;
    time = Time.SYSTEM;
    commitStrategyFactory =
        config ->
            (!config.upsertDelete() && !config.useStorageApi())
                ? new HighestUnblockedCommitStrategy()
                : new DefaultKafkaCommitStrategy();
  }

  /**
   * For testing purposes only; will never be called by the Kafka Connect framework.
   *
   * @param bigQuery {@link BigQuery} to use for testing (likely a mock)
   * @param storage {@link Storage} to use for testing (likely a mock)
   * @param schemaManager {@link SchemaManager} to use for testing (likely a mock)
   * @param time {@link Time} used to wait during backoff periods; should be mocked for testing
   * @see BigQuerySinkTask#BigQuerySinkTask()
   */
  @VisibleForTesting
  BigQuerySinkTask(
      BigQuery bigQuery,
      Storage storage,
      SchemaManager schemaManager,
      StorageWriteApiBase testStorageWriteApi,
      StorageApiBatchModeHandler testStorageApiBatchHandler,
      Time time,
      Function<BigQuerySinkTaskConfig, CommitStrategy> commitStrategyFactory) {
    this.bigQuerySupplier = () -> bigQuery;
    this.storageSupplier = () -> storage;
    this.schemaManagerSupplier = () -> schemaManager;
    this.testStorageWriteApi = testStorageWriteApi;
    this.testStorageApiBatchHandler = testStorageApiBatchHandler;
    this.time = time;
    this.commitStrategyFactory = commitStrategyFactory;
  }

//  @Override
//  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
//    // this method is no longer used.
//    if (upsertDelete) {
//      throw new ConnectException(
//          "This connector cannot perform upsert/delete on older versions of "
//              + "the Connect framework; please upgrade to version 0.10.2.0 or later");
//    } else if (useStorageApi && useStorageApiBatchMode) {
//      throw new ConnectException(
//          "This connector cannot use batch mode for the Storage Write API "
//              + "on older versions of the Connect framework; please upgrade to version 0.10.2.0 or later");
//    }
//
//    // Return immediately here since the executor will already be shutdown
//    if (stopped) {
//      // Still have to check for errors in order to prevent offsets being committed for records that
//      // we've failed to write
//      executor.maybeThrowEncounteredError();
//      return;
//    }
//
//    topicPartitionManager.resumeAll();
//  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> offsets) {

    maybeThrowErrors();

    Map<TopicPartition, OffsetAndMetadata> result = null;
    if (upsertDelete) {
      result = mergeBatches.latestOffsets();
    } else if (useStorageApiBatchMode) {
      result = batchHandler.getCommitableOffsets();
    } else {
      result = commitStrategy.preCommit(offsets);
    }
    checkQueueSize();
    logger.debug("Committable Offsets: {}", result);
    return result;
  }

  /**
   * Creates a TableWriterBuilder for the StorageAPI Strategy.
   *
   * @param table the table to create the writer for.
   * @return a TableWriterBuilder for the StorageAPI Strategy.
   */
  private TableWriterBuilder storageApiStrategyTableWriterBuilder(PartitionedTableId table) {
    StorageWriteApiWriter.Builder storageWriterBuilder =
        new StorageWriteApiWriter.Builder(storageApiWriter, table, recordConverter, batchHandler);
    if (trackPutAttempts) {
      storageWriterBuilder.withUlidSupplier(ULID_GENERATOR::nextULID);
    }
    return storageWriterBuilder;
  }

  /**
   * Creates a TableWriterBuilder for the GCS Batch strategy.
   *
   * @param table the table to create the writer for.
   * @return a TableWriterBuilder for the GCS Batch Strategy.
   */
  private TableWriterBuilder gscBatchStrategyTableWriterBuilder(
      SinkRecord record, PartitionedTableId table) {
    String topic = record.topic();
    long offset = record.kafkaOffset();
    String gcsBlobName = topic + "_" + uuid + "_" + Instant.now().toEpochMilli() + "_" + offset;
    String gcsFolderName = config.getString(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG);
    if (gcsFolderName != null && !gcsFolderName.isEmpty()) {
      gcsBlobName = gcsFolderName + "/" + gcsBlobName;
    }
    return new GcsBatchTableWriter.Builder(
        gcsToBqWriter,
        table.getBaseTableId(),
        config.getString(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG),
        gcsBlobName,
        recordConverter,
        errantRecordHandler);
  }

  /**
   * Creates a TableWriterBuilder for the Simple table writer strategy.
   *
   * @param table the table to create the writer for.
   * @return a TableWriterBuilder for the Simple table writer strategy.
   */
  private TableWriterBuilder simpleStrategyTableWriterBuilder(PartitionedTableId table) {
    TableWriter.Builder simpleTableWriterBuilder =
        new TableWriter.Builder(bigQueryWriter, table, recordConverter);
    if (upsertDelete) {
      simpleTableWriterBuilder.onFinish(
          rows -> mergeBatches.onRowWrites(table.getBaseTableId(), rows));
    }
    if (trackPutAttempts) {
      simpleTableWriterBuilder.withUlidSupplier(ULID_GENERATOR::nextULID);
    }
    return simpleTableWriterBuilder;
  }

  /**
   * Write the sink records to the proper tables.
   *
   * @param records the records to write.
   */
  private void writeSinkRecords(Collection<SinkRecord> records) {
    // Periodically poll for errors here instead of doing a stop-the-world check in flush()
    maybeThrowErrors();

    logger.debug("Putting {} records in the sink.", records.size());

    // create tableWriters
    Map<PartitionedTableId, TableWriterBuilder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null || config.isDeleteEnabled()) {
        PartitionedTableId table = recordTableResolver.getRecordTable(record);
        TableWriterBuilder tableWriterBuilder =
            tableWriterBuilders.computeIfAbsent(
                table,
                k -> {
                  if (useStorageApi) {
                    return storageApiStrategyTableWriterBuilder(table);
                  } else if (config
                      .getList(BigQuerySinkConfig.ENABLE_BATCH_CONFIG)
                      .contains(record.topic())) {
                    return gscBatchStrategyTableWriterBuilder(record, table);
                  } else {
                    return simpleStrategyTableWriterBuilder(table);
                  }
                });

        try {
          tableWriterBuilder.addRow(record, table.getBaseTableId());
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
      Runnable runnable = builder.build();
      if (runnable instanceof NeedsStreamState needsStreamState
          && !upsertDelete
          && !useStorageApiBatchMode) {
        executor.execute(new WrappedRunnable(needsStreamState, commitStrategy));
      } else {
        executor.execute(runnable);
      }
    }

    // check if we should pause topics
    checkQueueSize();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (trackPutAttempts) {
      recordConverter.setCurrentPutAttemptId(ULID_GENERATOR.nextULID());
    }
    try {
      writeSinkRecords(records);
      remainingRetries = config.maxRetries();
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
  // that it's safe to pause or resume partitions on the consumer.SinkRecordConverter(config,
  // mergeBatches, mergeQueries);
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
    return bigQuery.updateAndGet(bq -> bq != null ? bq : bigQuerySupplier.get());
  }

  private SchemaManager getSchemaManager() {
    return schemaManager.updateAndGet(sm -> sm != null ? sm : schemaManagerSupplier.get());
  }

  private BigQueryWriter getBigQueryWriter(ErrantRecordHandler errantRecordHandler) {
    BigQuery bigQuery = getBigQuery();
    if (upsertDelete) {
      return new UpsertDeleteBigQueryWriter(
          bigQuery,
          getSchemaManager(),
          autoCreateTables,
          mergeBatches.intermediateToDestinationTables(),
          errantRecordHandler,
          time,
          config);
    } else if (autoCreateTables || allowNewBigQueryFields || allowRequiredFieldRelaxation) {
      return new AdaptiveBigQueryWriter(
          bigQuery, getSchemaManager(), autoCreateTables, errantRecordHandler, time, config);
    } else {
      return new SimpleBigQueryWriter(bigQuery, errantRecordHandler, time, config);
    }
  }

  private Storage getGcs() {
    return storageSupplier.get();
  }

  private GcsToBqWriter getGcsWriter() {
    BigQuery bigQuery = getBigQuery();
    boolean attemptSchemaUpdate =
        allowNewBigQueryFields || allowRequiredFieldRelaxation || allowSchemaUnionization;
    // schemaManager shall only be needed for creating table or performing schema updates hence do
    // not fetch instance if not needed.
    boolean needsSchemaManager = autoCreateTables || attemptSchemaUpdate;
    SchemaManager schemaManager = needsSchemaManager ? getSchemaManager() : null;
    return new GcsToBqWriter(
        getGcs(),
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

  private static void populateLogicalConverterRegistry(BigQuerySinkTaskConfig config) {
    DebeziumLogicalConverters.initialize(config);
    KafkaLogicalConverters.initialize(config);
    if (config.getShouldUseAvroTemporalLogicalTypes()) {
      AvroLogicalConverters.initialize();
    }
  }

  @Override
  public void open(Collection<TopicPartition> partitions) {
    logger.debug("Assigned partitions: {}", partitions);
    commitStrategy.registerPartitions(partitions);
  }

  /**
   * @deprecated Use {@link #open(Collection)} for partition initialization.
   */
  @Deprecated
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    open(partitions);
  }


  @Override
  public void close(Collection<TopicPartition> partitions) {
    logger.debug("Unassigned partitions: {}", partitions);
    commitStrategy.unregisterPartitions(partitions);
  }

  /**
   * @deprecated Use {@link #close(Collection)} instead for partition cleanup.
   */
  @Deprecated
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    close(partitions);
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    config = new BigQuerySinkTaskConfig(properties);
    populateLogicalConverterRegistry(config);
    autoCreateTables = config.autoCreateTables();
    useStorageApi = config.useStorageApi();
    useStorageApiBatchMode = config.useStorageApiBatchMode();
    upsertDelete = config.upsertDelete();

    retry = config.maxRetries();
    retryWait = config.retryDelay().toMillis();
    allowNewBigQueryFields = config.allowNewFields();
    allowRequiredFieldRelaxation = config.allowFieldRelaxation();
    allowSchemaUnionization = config.allowSchemaUnionization();
    if (bigQuerySupplier == null) {
      bigQuerySupplier = () -> new GcpClientBuilder.BigQueryBuilder().withConfig(config).build();
    }
    if (storageSupplier == null) {
      storageSupplier = () -> new GcpClientBuilder.GcsBuilder().withConfig(config).build();
    }
    if (schemaManagerSupplier == null) {
      schemaManagerSupplier = () -> new SchemaManager(config, getBigQuery());
    }

    // Initialise errantRecordReporter
    ErrantRecordReporter errantRecordReporter = null;
    try {
      errantRecordReporter = context.errantRecordReporter(); // may be null if DLQ not enabled
    } catch (NoClassDefFoundError | NullPointerException e) {
      // Will occur in Connect runtimes earlier than 2.6
      logger.warn(
          "Connect versions prior to Apache Kafka 2.6 do not support the errant record "
              + "reporter");
    }
    errantRecordHandler = new ErrantRecordHandler(errantRecordReporter);

    if (upsertDelete) {
      String intermediateTableSuffix =
          String.format(
              "_%s_%d_%s_%d",
              config.getString(BigQuerySinkConfig.INTERMEDIATE_TABLE_SUFFIX_CONFIG),
              config.getInt(BigQuerySinkTaskConfig.TASK_ID_CONFIG),
              uuid,
              Instant.now().toEpochMilli());
      mergeBatches = new MergeBatches(intermediateTableSuffix);
    }

    bigQueryWriter = getBigQueryWriter(errantRecordHandler);
    gcsToBqWriter = getGcsWriter();

    // set the commit strategy.

    commitStrategy = commitStrategyFactory.apply(config);

    executor = configureExecutor(config);

    scheduledExecutor = new KcbqScheduledThreadPoolExecutor();
    scheduledExecutor.scheduleWithFixedDelay(
        new NamedRunnable(commitStrategy::cleanCommitStrategy, "Clean commits"),
        5,
        10,
        TimeUnit.SECONDS);

    topicPartitionManager = new TopicPartitionManager();
    recordTableResolver =
        new RecordTableResolver(
            config,
            mergeBatches,
            getBigQuery(),
            upsertDelete,
            useStorageApiBatchMode,
            useStorageApi);

    if (config.getBoolean(BigQuerySinkTaskConfig.GCS_BQ_TASK_CONFIG)) {
      startGcsToBqLoadTask();
    } else if (upsertDelete) {
      mergeQueries =
          new MergeQueries(
              config, mergeBatches, executor, getBigQuery(), getSchemaManager(), context);
      maybeStartMergeFlushTask();
    } else if (useStorageApi) {
      initializeStorageApiMode();
    }

    recordConverter = getConverter(config);
    remainingRetries = config.getInt(BigQuerySinkConfig.MAX_RETRIES_CONFIG);
    enableRetries = config.getBoolean(BigQuerySinkConfig.ENABLE_RETRIES_CONFIG);
    trackPutAttempts = config.getBoolean(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG);
    stopped = false;
  }

  private KcbqThreadPoolExecutor configureExecutor(BigQuerySinkConfig config) {
    BlockingQueue<Runnable> lastQueue = executor != null ? executor.getQueue() : null;

    final KcbqThreadPoolExecutor newExecutor = new KcbqThreadPoolExecutor(config.maxPoolSize());

    if (lastQueue != null) {
      List<Runnable> runnables =
          lastQueue.stream().filter(x -> x instanceof WrappedRunnable).toList();
      logger.info("Rescheduling {} runnable instances", runnables.size());
      runnables.forEach(newExecutor::execute);
    }

    return newExecutor;
  }

  private void initializeStorageApiMode() {
    if (testStorageWriteApi != null) {
      logger.info("Starting task with Test Storage Write API Stream");
      storageApiWriter = testStorageWriteApi;
      batchHandler = testStorageApiBatchHandler;
      int commitInterval = config.getInt(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG);
      scheduledExecutor.scheduleAtFixedRate(
          new NamedRunnable(batchHandler::refreshStreams, "Batch Loader"),
          commitInterval,
          commitInterval,
          TimeUnit.SECONDS);
    } else {
      boolean attemptSchemaUpdate = allowNewBigQueryFields || allowRequiredFieldRelaxation;
      BigQueryWriteSettings writeSettings =
          new GcpClientBuilder.BigQueryWriteSettingsBuilder().withConfig(config).build();
      if (useStorageApiBatchMode) {
        StorageWriteApiBatchApplicationStream writer =
            new StorageWriteApiBatchApplicationStream(
                retry,
                retryWait,
                writeSettings,
                autoCreateTables,
                errantRecordHandler,
                getSchemaManager(),
                attemptSchemaUpdate,
                config);
        storageApiWriter = writer;

        logger.info("Starting task with Storage Write API Batch Mode");
        batchHandler = new StorageApiBatchModeHandler(writer, config);

        int commitInterval = config.getInt(BigQuerySinkConfig.COMMIT_INTERVAL_SEC_CONFIG);
        logger.info(
            "Starting Load Executor for Storage Write API Batch Mode with {} seconds interval ",
            commitInterval);
        scheduledExecutor.scheduleAtFixedRate(
            new NamedRunnable(batchHandler::refreshStreams, "Batch Loader"),
            commitInterval,
            commitInterval,
            TimeUnit.SECONDS);
      } else {
        logger.info("Starting task with Storage Write API Default Stream");
        storageApiWriter =
            new StorageWriteApiDefaultStream(
                retry,
                retryWait,
                writeSettings,
                autoCreateTables,
                errantRecordHandler,
                getSchemaManager(),
                attemptSchemaUpdate,
                config);
      }
    }
  }

  /** If the executors are in an error state throw and exception. */
  private void maybeThrowErrors() {
    executor.maybeThrowEncounteredError();
    logger.debug(executor.status());
    scheduledExecutor.maybeThrowEncounteredError();
    logger.debug(scheduledExecutor.status());
    scheduledExecutor.taskNames().forEach(logger::debug);

    if (useStorageApiBatchMode && scheduledExecutor.isTerminated()) {
      throw new BigQueryStorageWriteApiConnectException(
          "Batch load handler is terminated, failing task as no data would be written to bigquery tables!");
    }
  }

  private void startGcsToBqLoadTask() {
    logger.info("Attempting to start GCS Load Executor.");
    // use a single thread to ensure that no more than one thread is attempting to send
    // data to BigQuery otherwise duplicates may occur.
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
        throw new ConnectException(
            String.format(
                "Bucket '%s' does not exist; Create the bucket manually, or set '%s' to true",
                bucketName, BigQuerySinkConfig.AUTO_CREATE_BUCKET_CONFIG));
      }
    }
    GcsToBqLoadRunnable loadRunnable = new GcsToBqLoadRunnable(getBigQuery(), bucket);

    int intervalSec = config.getInt(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG);
    scheduledExecutor.scheduleAtFixedRate(
        new NamedRunnable(loadRunnable, "GCS Loader"), 0, intervalSec, TimeUnit.SECONDS);
    scheduledExecutor.execute( () -> logger.warn("Schedule executor running"));
    scheduledExecutor.scheduleAtFixedRate(
            () -> logger.warn("Scheduled schedule executor running"), 0, intervalSec, TimeUnit.SECONDS);
  }

  private void maybeStartMergeFlushTask() {
    long intervalMs = config.getLong(BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG);
    if (intervalMs == -1) {
      logger.info(
          "{} is set to -1; periodic merge flushes are disabled",
          BigQuerySinkConfig.MERGE_INTERVAL_MS_CONFIG);
      return;
    }
    logger.info("Attempting to start upsert/delete load executor");
    scheduledExecutor.scheduleAtFixedRate(
        new NamedRunnable(mergeQueries::mergeFlushAll, "Merge Query flush"),
        intervalMs,
        intervalMs,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void stop() {
    try {
      maybeStopExecutor(scheduledExecutor, "load executor");
      maybeStopExecutor(executor, "table write executor");
      if (upsertDelete) {
        mergeBatches
            .intermediateTables()
            .forEach(
                table -> {
                  logger.debug("Deleting {}", intTable(table));
                  getBigQuery().delete(table);
                });
      } else if (useStorageApi) {
        storageApiWriter.shutdown();
      }
    } finally {
      stopped = true;
    }

    // clear the atomics because a start may have a new configuration
    schemaManager.set(null);
    bigQuery.set(null);

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
      context.pause(assignment.toArray(new TopicPartition[0]));
    }

    public void resumeAll() {
      if (isPaused) {
        long now = System.currentTimeMillis();
        logger.info("Resumed all partitions after {}ms", now - lastChangeMs);
        isPaused = false;
        lastChangeMs = now;
      }
      Set<TopicPartition> assignment = context.assignment();
      context.resume(assignment.toArray(new TopicPartition[0]));
    }
  }

  class WrappedRunnable implements Runnable, StreamStateContainer {
    private final NeedsStreamState tableWriter;
    private StreamState streamState;
    private boolean aborted;

    public WrappedRunnable(NeedsStreamState tableWriter, CommitStrategy strategy) {
      this.tableWriter = tableWriter;
      streamState = StreamState.FINALISED;
      strategy.queue(tableWriter.records(), this);
    }

    @Override
    public boolean stateIs(StreamState state) {
      return streamState == state;
    }

    @Override
    public void abort() {
      aborted = true;
    }

    @Override
    public void run() {
      if (aborted) {
        streamState = StreamState.FINALISED;
      } else {
        logger.debug("WrappedRunnable Start {}", tableWriter.hashCode());
        MDC.put("WrappedRunnable", Integer.toString(tableWriter.hashCode()));

        try {
          tableWriter.run();
          streamState = StreamState.COMMITTED;
        } catch (RuntimeException e) {
          executor.logAndRecord(e);
        } finally {
          logger.debug("WrappedRunnable End {}", tableWriter.hashCode());
          MDC.remove("WrappedRunnable");
        }
      }
    }
  }
}
