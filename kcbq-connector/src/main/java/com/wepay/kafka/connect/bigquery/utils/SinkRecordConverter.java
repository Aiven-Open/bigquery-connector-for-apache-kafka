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

package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.MergeQueries;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for converting a {@link SinkRecord SinkRecord} to {@link InsertAllRequest.RowToInsert BigQuery row}
 */
public class SinkRecordConverter {
  private static final Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

  private final BigQuerySinkConfig config;
  private final MergeBatches mergeBatches;
  private final MergeQueries mergeQueries;

  private final RecordConverter<Map<String, Object>> recordConverter;
  private final long mergeRecordsThreshold;
  private final boolean useMessageTimeDatePartitioning;
  private final boolean usePartitionDecorator;

  /** Set by {@link com.wepay.kafka.connect.bigquery.BigQuerySinkTask#put} at the start of each
   * put() invocation when {@code trackPutAttempts} is enabled. Null otherwise. */
  private volatile String currentPutAttemptId = null;


  public SinkRecordConverter(BigQuerySinkConfig config,
                             MergeBatches mergeBatches, MergeQueries mergeQueries) {
    this.config = config;
    this.mergeBatches = mergeBatches;
    this.mergeQueries = mergeQueries;

    this.recordConverter = config.getRecordConverter();
    this.mergeRecordsThreshold = config.getMergeThreshold();
    this.useMessageTimeDatePartitioning = config.useMessageTime();
    this.usePartitionDecorator = config.appendPartitionDecorator();
  }

  /**
   * Called by {@link com.wepay.kafka.connect.bigquery.BigQuerySinkTask#put} once per put()
   * invocation, before any rows are constructed. The ID is embedded in each row's kafka metadata
   * struct when {@code trackPutAttempts} is enabled, allowing downstream consumers to distinguish
   * rows produced by different put() attempts.
   *
   * @param id ULID string for the current put() invocation, or {@code null} to clear.
   */
  public void setCurrentPutAttemptId(String id) {
    this.currentPutAttemptId = id;
  }

  public InsertAllRequest.RowToInsert getRecordRow(SinkRecord record, TableId table) {
    return getRecordRow(record, table, currentPutAttemptId);
  }

  /**
   * Converts a record to a BigQuery row using an explicitly supplied write-attempt ID instead of
   * the shared {@code currentPutAttemptId} field. Use this overload from executor threads (e.g.,
   * inside {@code BigQueryWriter.writeRows()}) to avoid the race condition that arises when
   * multiple {@code TableWriter} threads concurrently read and write the shared volatile field.
   *
   * @param record         the Kafka record to convert
   * @param table          the target BigQuery table
   * @param writeAttemptId the write-attempt ID to embed, or {@code null} if tracking is disabled
   */
  public InsertAllRequest.RowToInsert getRecordRow(SinkRecord record, TableId table, String writeAttemptId) {
    Map<String, Object> convertedRecord = config.isUpsertDeleteEnabled()
        ? getUpsertDeleteRow(record, table, writeAttemptId)
        : getRegularRow(record, writeAttemptId);

    return InsertAllRequest.RowToInsert.of(getRowId(record), convertedRecord);
  }

  private Map<String, Object> getUpsertDeleteRow(SinkRecord record, TableId table, String writeAttemptId) {
    // Unconditionally allow tombstone records if delete is enabled.
    Map<String, Object> convertedValue = config.getBoolean(config.DELETE_ENABLED_CONFIG) && record.value() == null
        ? null
        : recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

    if (convertedValue != null) {
      config.getKafkaDataFieldName().ifPresent(
          fieldName -> convertedValue.put(fieldName, buildKafkaDataRecord(record, writeAttemptId))
      );
    }

    Map<String, Object> result = new HashMap<>();
    long totalBatchSize = mergeBatches.addToBatch(record, table, result);
    if (mergeRecordsThreshold != -1 && totalBatchSize >= mergeRecordsThreshold) {
      logger.debug("Triggering merge flush for table {} since the size of its current batch has "
              + "exceeded the configured threshold of {}}",
          table, mergeRecordsThreshold);
      mergeQueries.mergeFlush(table);
    }

    Map<String, Object> convertedKey = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
    if (convertedKey == null) {
      throw new ConnectException("Record keys must be non-null when upsert/delete is enabled");
    }

    result.put(MergeQueries.INTERMEDIATE_TABLE_KEY_FIELD_NAME, convertedKey);
    result.put(MergeQueries.INTERMEDIATE_TABLE_VALUE_FIELD_NAME, convertedValue);
    result.put(MergeQueries.INTERMEDIATE_TABLE_ITERATION_FIELD_NAME, totalBatchSize);
    if (usePartitionDecorator && useMessageTimeDatePartitioning) {
      if (record.timestampType() == TimestampType.NO_TIMESTAMP_TYPE) {
        throw new ConnectException(
            "Message has no timestamp type, cannot use message timestamp to partition.");
      }
      result.put(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, record.timestamp());
    } else {
      // Provide a value for this column even if it's not used for partitioning in the destination
      // table, so that it can be used to deduplicate rows during merge flushes
      result.put(MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME, System.currentTimeMillis() / 1000);
    }

    return maybeSanitize(result);
  }

  public Map<String, Object> getRegularRow(SinkRecord record) {
    return getRegularRow(record, currentPutAttemptId);
  }

  public Map<String, Object> getRegularRow(SinkRecord record, String writeAttemptId) {
    Map<String, Object> result = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

    config.getKafkaDataFieldName().ifPresent(fieldName ->
            result.put(fieldName, buildKafkaDataRecord(record, writeAttemptId)));

    config.getKafkaKeyFieldName().ifPresent(fieldName ->
            result.put(fieldName, recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY)));

    return maybeSanitize(result);
  }

  private Map<String, Object> maybeSanitize(Map<String, Object> convertedRecord) {
    return config.getBoolean(config.SANITIZE_FIELD_NAME_CONFIG)
        ? FieldNameSanitizer.replaceInvalidKeys(convertedRecord)
        : convertedRecord;
  }

  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset());
  }

  private String maybeGetOriginalTopic(SinkRecord kafkaConnectRecord) {
    if (config.useOriginalValues()) {
      return kafkaConnectRecord.originalTopic();
    } else {
      return kafkaConnectRecord.topic();
    }
  }

  private Integer maybeGetOriginalKafkaPartition(SinkRecord kafkaConnectRecord) {
    if (config.useOriginalValues()) {
      return kafkaConnectRecord.originalKafkaPartition();
    } else {
      return kafkaConnectRecord.kafkaPartition();
    }
  }

  private long maybeGetOriginalKafkaOffset(SinkRecord kafkaConnectRecord) {
    if (config.useOriginalValues()) {
      return kafkaConnectRecord.originalKafkaOffset();
    } else {
      return kafkaConnectRecord.kafkaOffset();
    }
  }

  /**
   * Construct a map of Kafka Data record, optionally including a put-attempt identifier.
   *
   * <p>When {@code TRACK_PUT_ATTEMPTS} is enabled and {@code putAttemptId} is non-null, the map
   * includes a {@code putAttemptId} entry so that rows constructed during different
   * {@code put()} invocations can be distinguished downstream.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @param putAttemptId ULID string generated at the start of the enclosing {@code put()} call,
   *                     or {@code null} to omit the field.
   * @return HashMap which contains the values of kafka topic, partition, offset, insertTime,
   *         and optionally putAttemptId.
   */
  public Map<String, Object> buildKafkaDataRecord(SinkRecord kafkaConnectRecord,
                                                  String putAttemptId) {
    HashMap<String, Object> kafkaData = new HashMap<>();
    kafkaData.put(SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, maybeGetOriginalTopic(kafkaConnectRecord));
    kafkaData.put(SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME, maybeGetOriginalKafkaPartition(kafkaConnectRecord));
    kafkaData.put(SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME, maybeGetOriginalKafkaOffset(kafkaConnectRecord));
    if (config.useStorageWriteApi()) {
      kafkaData.put(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() * 1000);
    } else {
      kafkaData.put(SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
    }
    if (config.trackPutAttempts() && putAttemptId != null) {
      kafkaData.put(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);
    }
    return kafkaData;
  }


}
