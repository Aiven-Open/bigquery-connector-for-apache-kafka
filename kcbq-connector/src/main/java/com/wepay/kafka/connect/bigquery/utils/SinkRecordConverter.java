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
import com.google.common.annotations.VisibleForTesting;
import com.wepay.kafka.connect.bigquery.MergeQueries;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for converting a {@link SinkRecord SinkRecord} to {@link InsertAllRequest.RowToInsert
 * BigQuery row}
 */
public final class SinkRecordConverter {
  private static final Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  public static final String CDC_CHANGE_TYPE_FIELD = "_CHANGE_TYPE";
  public static final String CDC_CHANGE_SEQUENCE_NUMBER_FIELD = "_CHANGE_SEQUENCE_NUMBER";
  public static final String CDC_CHANGE_TYPE_UPSERT = "UPSERT";
  public static final String CDC_CHANGE_TYPE_DELETE = "DELETE";
  public static final String DELETED_PSEUDO_COLUMN = BigQuerySchemaConverter.DELETED_PSEUDO_COLUMN;

  private final BigQuerySinkConfig config;
  private final MergeBatches mergeBatches;
  private final MergeQueries mergeQueries;

  private final RecordConverter<Map<String, Object>> recordConverter;
  private final long mergeRecordsThreshold;
  private final boolean useMessageTimeDatePartitioning;
  private final boolean usePartitionDecorator;

  /**
   * Set by {@link com.wepay.kafka.connect.bigquery.BigQuerySinkTask#put} at the start of each put()
   * invocation when {@code trackPutAttempts} is enabled. Null otherwise.
   */
  private volatile String currentPutAttemptId = null;

  public SinkRecordConverter(
      BigQuerySinkConfig config, MergeBatches mergeBatches, MergeQueries mergeQueries) {
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
   * @param record the Kafka record to convert
   * @param table the target BigQuery table
   * @param writeAttemptId the write-attempt ID to embed, or {@code null} if tracking is disabled
   */
  public InsertAllRequest.RowToInsert getRecordRow(
      SinkRecord record, TableId table, String writeAttemptId) {
    Map<String, Object> convertedRecord =
        config.isUpsertEnabled() || config.isDeleteEnabled()
            ? getUpsertDeleteRow(record, table, writeAttemptId)
            : getRegularRow(record, writeAttemptId);

    return InsertAllRequest.RowToInsert.of(getRowId(record), convertedRecord);
  }

  /**
   * Create the converted row for the case where upsert or delete are enabled.
   *
   * @param record the record to convert.
   * @param table the table to write to.
   * @param writeAttemptId the write ID.
   * @return the map of the converted record.
   */
  private Map<String, Object> getUpsertDeleteRow(
      SinkRecord record, TableId table, String writeAttemptId) {
    // Unconditionally allow tombstone records if delete is enabled.
    Map<String, Object> convertedValue =
        config.isDeleteEnabled() && record.value() == null
            ? null
            : recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

    if (convertedValue != null) {
      config
          .getKafkaDataFieldName()
          .ifPresent(
              fieldName ->
                  convertedValue.put(fieldName, buildKafkaDataRecord(record, writeAttemptId)));
    }

    Map<String, Object> result = new HashMap<>();
    long totalBatchSize = mergeBatches.addToBatch(record, table, result);
    if (mergeRecordsThreshold != -1 && totalBatchSize >= mergeRecordsThreshold) {
      logger.debug(
          "Triggering merge flush for table {} since the size of its current batch has "
              + "exceeded the configured threshold of {}}",
          table,
          mergeRecordsThreshold);
      mergeQueries.mergeFlush(table);
    }

    Map<String, Object> convertedKey =
        recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
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
      result.put(
          MergeQueries.INTERMEDIATE_TABLE_PARTITION_TIME_FIELD_NAME,
          System.currentTimeMillis() / 1000);
    }

    return maybeSanitize(result);
  }

  /**
   * Converts a SinkRecord to a regular row using the current putAttemptId.
   *
   * @param record the record to convert.
   * @return the map of fields to values.
   */
  public Map<String, Object> getRegularRow(SinkRecord record) {
    return getRegularRow(record, currentPutAttemptId);
  }

  /**
   * Converts a SinkRecord to a regular row using the specified putAttemptId.
   *
   * @param record the record to convert.
   * @param writeAttemptId the write attempt id to use.
   * @return the map of fields to values.
   */
  public Map<String, Object> getRegularRow(SinkRecord record, String writeAttemptId) {
    logger.trace(
        "getRegularRow INPUT - Topic: {}, Offset: {}, Value: {}",
        record.topic(),
        record.kafkaOffset(),
        record.value());

    // if delete is enabled and there is a null value then the record was deleted.  In other cases a
    // null value may be appropriate and the converter will determine if there are any issues.
    Map<String, Object> result =
        config.getBoolean(config.DELETE_ENABLED_CONFIG) && record.value() == null
            ? new HashMap<>()
            : recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

    config
        .getKafkaDataFieldName()
        .ifPresent(
            fieldName -> result.put(fieldName, buildKafkaDataRecord(record, writeAttemptId)));

    config
        .getKafkaKeyFieldName()
        .ifPresent(
            fieldName -> {
              Map<String, Object> keyData =
                  recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
              if (fieldName.isEmpty()) {
                result.putAll(keyData);
              } else {
                result.put(fieldName, keyData);
              }
            });

    logger.trace(
        "getRegularRow OUTPUT - Topic: {}, Offset: {}, Result Map: {}",
        record.topic(),
        record.kafkaOffset(),
        result);
    return maybeSanitize(result);
  }

  /**
   * Converts a SinkRecord to a CDC row using the shared {@code currentPutAttemptId}.
   *
   * @param record the record to convert.
   * @return the map of fields to values representing the CDC row.
   */
  public Map<String, Object> getCdcRow(SinkRecord record) {
    return getCdcRow(record, currentPutAttemptId);
  }

  /**
   * Converts a SinkRecord to a CDC row using the specified writeAttemptId. Extracts both Key fields
   * (as primary key columns) and Value fields, handles tombstone records for deletes, and adds
   * Kafka metadata fields if configured.
   *
   * @param record the record to convert.
   * @param writeAttemptId the write attempt id to use.
   * @return the map of fields to values representing the CDC row.
   */
  public Map<String, Object> getCdcRow(SinkRecord record, String writeAttemptId) {
    logger.trace(
        "getCdcRow INPUT - Topic: {}, Offset: {}, Key: {}, Value: {}",
        record.topic(),
        record.kafkaOffset(),
        record.key(),
        record.value());
    Map<String, Object> result = new HashMap<>();

    // 1. Extract the Key fields
    Map<String, Object> convertedKey =
        recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
    if (convertedKey != null) {
      result.putAll(convertedKey);
    } else if (record.value() == null) {
      // If the value is null, it's a DELETE. We cannot delete a row if we don't know
      // its key!
      throw new ConnectException("Record keys must be non-null when upsert/delete is enabled");
    }

    // 2. Extract the Value fields (only if it's not a tombstone record)
    Map<String, Object> convertedValue = null;
    if (record.value() != null) {
      convertedValue = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);
      if (convertedValue != null) {
        result.putAll(convertedValue); // Merges value fields into the root of the map
      }
    }

    // 3. Optional Kafka metadata (e.g. insert time, topic, partition, offset)
    config
        .getKafkaDataFieldName()
        .ifPresent(
            fieldName -> {
              Map<String, Object> kafkaDataField = buildKafkaDataRecord(record, writeAttemptId);
              result.put(fieldName, kafkaDataField);
            });

    // 4. Set the CDC metadata columns
    String changeType = CDC_CHANGE_TYPE_UPSERT;
    if (record.value() == null) {
      logger.debug(
          "Tombstone record (null value) detected for key {} at offset {}",
          record.key(),
          record.kafkaOffset());
      changeType = CDC_CHANGE_TYPE_DELETE;
    } else if (convertedValue != null) {
      Object deletedVal = convertedValue.get(DELETED_PSEUDO_COLUMN);
      if (deletedVal instanceof Boolean && (Boolean) deletedVal) {
        changeType = CDC_CHANGE_TYPE_DELETE;
      } else if (deletedVal instanceof String && Boolean.parseBoolean((String) deletedVal)) {
        changeType = CDC_CHANGE_TYPE_DELETE;
      }
    }
    result.put(CDC_CHANGE_TYPE_FIELD, changeType);
    // Strip the transient __deleted metadata field to prevent BigQuery ingestion crashes due to
    // unknown fields.
    result.remove(DELETED_PSEUDO_COLUMN);

    String customSeqField = config.getCdcChangeSequenceNumberField().orElse(null);
    String seqNumber;
    Long recordTimestamp = record.timestamp();
    long ts =
        (recordTimestamp != null && recordTimestamp >= 0)
            ? recordTimestamp
            : System.currentTimeMillis();

    if (customSeqField != null && !customSeqField.trim().isEmpty()) {
      if ("_KAFKA_TIMESTAMP".equalsIgnoreCase(customSeqField)) {
        // Option 1 (Default): timestamp / offset / partition
        seqNumber = formatDefaultSequence(ts, record);
      } else {
        Object seqValue = null;
        if (convertedValue != null && convertedValue.get(customSeqField) != null) {
          seqValue = convertedValue.get(customSeqField);
        } else if (convertedKey != null && convertedKey.get(customSeqField) != null) {
          seqValue = convertedKey.get(customSeqField);
        } else if (record.headers() != null) {
          Header header = record.headers().lastWithName(customSeqField);
          if (header != null && header.value() != null) {
            Object headerVal = header.value();
            if (headerVal instanceof byte[]) {
              seqValue = new String((byte[]) headerVal, StandardCharsets.UTF_8);
            } else {
              seqValue = headerVal;
            }
          }
        }

        if (seqValue != null) {
          // Option 2 (Custom sequence number): customsequence / timestamp / offset / partition
          seqNumber = convertToCustomHexSequence(seqValue, ts, record);
        } else {
          // Fallback to Option 1 (Default) if custom field is missing (e.g. raw tombstones)
          seqNumber = formatDefaultSequence(ts, record);
        }
      }
    } else {
      // Option 1 (Default): timestamp / offset / partition
      seqNumber = formatDefaultSequence(ts, record);
    }
    result.put(CDC_CHANGE_SEQUENCE_NUMBER_FIELD, seqNumber);

    logger.trace(
        "getCdcRow OUTPUT - Topic: {}, Partition: {}, Offset: {}, Result Map: {}",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset(),
        result);

    // 5. Sanitize column names if the user turned on the sanitize option (replacing
    // spaces/special characters)
    return maybeSanitize(result);
  }

  /**
   * Formats the default sequence as "[16-hex-timestamp]/[16-hex-offset]/[8-hex-partition]".
   *
   * @param ts The record timestamp (or current time millis)
   * @param record The sink record providing offset and partition
   * @return The formatted default hex sequence string
   */
  private String formatDefaultSequence(long ts, SinkRecord record) {
    return String.format("%016X/%016X/%08X", ts, record.kafkaOffset(), record.kafkaPartition());
  }

  /**
   * Formats the custom sequence as
   * "[16-hex-customsequence]/[16-hex-timestamp]/[16-hex-offset]/[8-hex-partition]". BigQuery's
   * Storage Write API parses slash-separated segments of up to 16 hex digits each and preserves
   * strict multi-segment lexicographical ordering.
   *
   * @param seqValue The raw sequence number or timestamp string
   * @param ts The record timestamp (or current time millis)
   * @param record The sink record providing offset and partition
   * @return The formatted composite hex sequence string
   */
  private String convertToCustomHexSequence(Object seqValue, long ts, SinkRecord record) {
    if (seqValue == null) {
      return formatDefaultSequence(ts, record);
    }

    Long seqLong = null;

    if (seqValue instanceof Number) {
      seqLong = ((Number) seqValue).longValue();
    } else {
      String strVal = seqValue.toString().trim();
      // Try to parse as raw Long first (e.g. "1785367800000")
      try {
        seqLong = Long.parseLong(strVal);
      } catch (NumberFormatException e) {
        // Not a raw number. Try parsing as a timestamp string.
        try {
          String normalized = strVal.replace(' ', 'T');
          Instant instant;
          if (normalized.endsWith("Z")) {
            instant = Instant.parse(normalized);
          } else {
            try {
              instant = OffsetDateTime.parse(normalized).toInstant();
            } catch (DateTimeParseException ex) {
              instant = LocalDateTime.parse(normalized).toInstant(ZoneOffset.UTC);
            }
          }
          seqLong = instant.toEpochMilli();
        } catch (Exception ex) {
          // If timestamp parsing fails, fallback to character hex-encoding with ts + offset +
          // partition
          return hexEncodeCustomSequence(strVal, ts, record);
        }
      }
    }

    if (seqLong != null) {
      return String.format(
          "%016X/%016X/%016X/%08X", seqLong, ts, record.kafkaOffset(), record.kafkaPartition());
    }
    return formatDefaultSequence(ts, record);
  }

  private String hexEncodeCustomSequence(String strVal, long ts, SinkRecord record) {
    byte[] bytes = strVal.getBytes(StandardCharsets.UTF_8);
    StringBuilder hexBuilder = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hexBuilder.append(HEX_CHARS[(b >> 4) & 0x0F]);
      hexBuilder.append(HEX_CHARS[b & 0x0F]);
    }
    String hexStr = hexBuilder.toString();
    String customSegment = hexStr.length() > 16 ? hexStr.substring(0, 16) : hexStr;
    return String.format(
        "%s/%016X/%016X/%08X", customSegment, ts, record.kafkaOffset(), record.kafkaPartition());
  }

  public boolean isCdcEnabled() {
    boolean enabled =
        config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG) && config.isUpsertDeleteEnabled();
    logger.trace(
        "isCdcEnabled check - USE_STORAGE_WRITE_API: {}, isUpsertDeleteEnabled: {}, Result: {}",
        config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG),
        config.isUpsertDeleteEnabled(),
        enabled);
    return enabled;
  }

  /**
   * Converts field names to BigQuery acceptable names if configured to do so.
   *
   * @param convertedRecord the record to sanitize.
   * @return the sanitized record if configured to do so, otherwise the unmodified {@code
   *     convertedRecord}.
   */
  private Map<String, Object> maybeSanitize(Map<String, Object> convertedRecord) {
    return config.sanitizeFieldNames()
        ? FieldNameSanitizer.replaceInvalidKeys(convertedRecord)
        : convertedRecord;
  }

  /**
   * Generates the row ID for the BigQuery row. This is constructed from the topic, partition and
   * offset of the record. Values are not affected by the use original values configuration option.
   *
   * @param record The sink record to generate the id for.
   * @return the ID for the row.
   */
  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
  }

  /**
   * Returns the original topic or the current topic as appropriate.
   *
   * @param kafkaConnectRecord the record to get the topic from.
   * @return the original topic or the current topic as appropriate.
   */
  private String maybeGetOriginalTopic(SinkRecord kafkaConnectRecord) {
    if (config.useOriginalValues()) {
      return kafkaConnectRecord.originalTopic();
    } else {
      return kafkaConnectRecord.topic();
    }
  }

  /**
   * Returns the original partition or the current partition as appropriate.
   *
   * @param kafkaConnectRecord the record to get the partition from.
   * @return the original partition or the current partition as appropriate.
   */
  private Integer maybeGetOriginalKafkaPartition(SinkRecord kafkaConnectRecord) {
    if (config.useOriginalValues()) {
      return kafkaConnectRecord.originalKafkaPartition();
    } else {
      return kafkaConnectRecord.kafkaPartition();
    }
  }

  /**
   * Returns the original offset or the current offset as appropriate.
   *
   * @param kafkaConnectRecord the record to get the offset from.
   * @return the original offset or the current offset as appropriate.
   */
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
   * includes a {@code putAttemptId} entry so that rows constructed during different {@code put()}
   * invocations can be distinguished downstream.
   *
   * <p>Note: Future versions of this method will be package private.
   *
   * @param kafkaConnectRecord Kafka sink record to build kafka data from.
   * @param putAttemptId ULID string generated at the start of the enclosing {@code put()} call, or
   *     {@code null} to omit the field.
   * @return HashMap which contains the values of kafka topic, partition, offset, insertTime, and
   *     optionally putAttemptId.
   */
  @VisibleForTesting
  public Map<String, Object> buildKafkaDataRecord(
      SinkRecord kafkaConnectRecord, String putAttemptId) {
    HashMap<String, Object> kafkaData = new HashMap<>();
    kafkaData.put(
        SchemaManager.KAFKA_DATA_TOPIC_FIELD_NAME, maybeGetOriginalTopic(kafkaConnectRecord));
    kafkaData.put(
        SchemaManager.KAFKA_DATA_PARTITION_FIELD_NAME,
        maybeGetOriginalKafkaPartition(kafkaConnectRecord));
    kafkaData.put(
        SchemaManager.KAFKA_DATA_OFFSET_FIELD_NAME,
        maybeGetOriginalKafkaOffset(kafkaConnectRecord));
    if (config.useStorageWriteApi()) {
      kafkaData.put(
          SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() * 1000);
    } else {
      kafkaData.put(
          SchemaManager.KAFKA_DATA_INSERT_TIME_FIELD_NAME, System.currentTimeMillis() / 1000.0);
    }
    if (config.trackPutAttempts() && putAttemptId != null) {
      kafkaData.put(SchemaManager.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME, putAttemptId);
    }
    return kafkaData;
  }
}
