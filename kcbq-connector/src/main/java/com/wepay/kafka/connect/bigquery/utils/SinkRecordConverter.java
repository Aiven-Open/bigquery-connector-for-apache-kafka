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
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import java.time.Instant;
import java.time.OffsetDateTime;
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

  private final BigQuerySinkTaskConfig config;
  private final MergeBatches mergeBatches;
  private final MergeQueries mergeQueries;

  private final RecordConverter<Map<String, Object>> recordConverter;
  private final long mergeRecordsThreshold;
  private final boolean useMessageTimeDatePartitioning;
  private final boolean usePartitionDecorator;

  /** Set by {@link com.wepay.kafka.connect.bigquery.BigQuerySinkTask#put} at the start of each
   * put() invocation when {@code trackPutAttempts} is enabled. Null otherwise. */
  private volatile String currentPutAttemptId = null;


  public SinkRecordConverter(BigQuerySinkTaskConfig config,
                             MergeBatches mergeBatches, MergeQueries mergeQueries) {
    this.config = config;
    this.mergeBatches = mergeBatches;
    this.mergeQueries = mergeQueries;

    this.recordConverter = config.getRecordConverter();
    this.mergeRecordsThreshold = config.getLong(config.MERGE_RECORDS_THRESHOLD_CONFIG);
    this.useMessageTimeDatePartitioning =
        config.getBoolean(config.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG);
    this.usePartitionDecorator =
        config.getBoolean(config.BIGQUERY_PARTITION_DECORATOR_CONFIG);
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
          fieldName -> convertedValue.put(fieldName, KafkaDataBuilder.buildKafkaDataRecord(record, writeAttemptId))
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
    logger.info("getRegularRow INPUT - Topic: {}, Offset: {}, Value: {}", 
        record.topic(), record.kafkaOffset(), record.value());
    Map<String, Object> result = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);

    config.getKafkaDataFieldName().ifPresent(fieldName -> {
      Map<String, Object> kafkaDataField = config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG)
          ? KafkaDataBuilder.buildKafkaDataRecordStorageApi(record, writeAttemptId)
          : KafkaDataBuilder.buildKafkaDataRecord(record, writeAttemptId);
      result.put(fieldName, kafkaDataField);
    });

    config.getKafkaKeyFieldName().ifPresent(fieldName -> {
      Map<String, Object> keyData = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
      result.put(fieldName, keyData);
    });

    logger.info("getRegularRow OUTPUT - Topic: {}, Offset: {}, Result Map: {}", 
        record.topic(), record.kafkaOffset(), result);
    return maybeSanitize(result);
  }

  public Map<String, Object> getCdcRow(SinkRecord record) {
    return getCdcRow(record, currentPutAttemptId);
  }

  public Map<String, Object> getCdcRow(SinkRecord record, String writeAttemptId) {
    logger.info("getCdcRow INPUT - Topic: {}, Offset: {}, Key: {}, Value: {}", 
        record.topic(), record.kafkaOffset(), record.key(), record.value());
    Map<String, Object> result = new HashMap<>();

    // 1. Extract the Key fields
    Map<String, Object> convertedKey = recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY);
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
    config.getKafkaDataFieldName().ifPresent(fieldName -> {
      Map<String, Object> kafkaDataField = config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG)
          ? KafkaDataBuilder.buildKafkaDataRecordStorageApi(record, writeAttemptId)
          : KafkaDataBuilder.buildKafkaDataRecord(record, writeAttemptId);
      result.put(fieldName, kafkaDataField);
    });

    // 4. Set the CDC metadata columns
    String changeType = "UPSERT";
    if (record.value() == null) {
      changeType = "DELETE";
    } else if (convertedValue != null) {
      Object deletedVal = convertedValue.get("__deleted");
      if (deletedVal instanceof Boolean && (Boolean) deletedVal) {
        changeType = "DELETE";
      } else if (deletedVal instanceof String && Boolean.parseBoolean((String) deletedVal)) {
        changeType = "DELETE";
      }
    }
    result.put("_CHANGE_TYPE", changeType);
    // Strip the transient __deleted metadata field to prevent BigQuery ingestion crashes due to unknown fields.
    result.remove("__deleted");
    String customSeqField = config.getCdcChangeSequenceNumberField().orElse(null);
    Object seqValue = null;

    if (customSeqField != null && !customSeqField.isEmpty()) {
      if ("_KAFKA_TIMESTAMP".equalsIgnoreCase(customSeqField)) {
        seqValue = record.timestamp();
      } else {
        // 1. Try reading from Value Payload (if not null)
        if (convertedValue != null) {
          seqValue = convertedValue.get(customSeqField);
        }
        // 2. Try reading from Key Payload
        if (seqValue == null && convertedKey != null) {
          seqValue = convertedKey.get(customSeqField);
        }
      }
    }

    if (seqValue != null) {
      result.put("_CHANGE_SEQUENCE_NUMBER", convertToHexSequence(seqValue, record));
    } else {
      if (customSeqField != null && !customSeqField.isEmpty()) {
        // If the custom sequence field is missing (e.g. on raw tombstone deletes without SMT rewrite),
        // fall back to the Kafka record timestamp to ensure the delete event is ordered correctly relative to inserts.
        Long fallbackTimestamp = record.timestamp();
        if (fallbackTimestamp == null || fallbackTimestamp < 0) {
          fallbackTimestamp = System.currentTimeMillis();
        }
        result.put("_CHANGE_SEQUENCE_NUMBER", convertToHexSequence(fallbackTimestamp, record));
      } else {
        result.put("_CHANGE_SEQUENCE_NUMBER", String.format("%016x", record.kafkaOffset()));
      }
    }

    logger.info("getCdcRow OUTPUT - Topic: {}, Offset: {}, Result Map: {}", 
        record.topic(), record.kafkaOffset(), result);

    // 5. Sanitize column names if the user turned on the sanitize option (replacing
    // spaces/special characters)
    return maybeSanitize(result);
  }

  /**
   * Formats the sequence value into a 16-character hexadecimal string, and appends the 16-character 
   * hexadecimal Kafka offset as a tie-breaker segment (e.g., "[hex-seq]/[hex-offset]").
   * This ensures deterministic chronological sorting in BigQuery, even when multiple events share 
   * the same timestamp.
   *
   * @param seqValue The raw sequence number or timestamp
   * @param record The sink record providing the offset
   * @return The formatted composite hex sequence string
   */
  private String convertToHexSequence(Object seqValue, SinkRecord record) {
    if (seqValue == null) {
      return null;
    }

    String seqHex = null;

    if (seqValue instanceof Number) {
      seqHex = String.format("%016x", ((Number) seqValue).longValue());
    } else {
      String strVal = seqValue.toString().trim();
      // Try to parse as raw Long first (e.g. "1785367800000")
      try {
        seqHex = String.format("%016x", Long.parseLong(strVal));
      } catch (NumberFormatException e) {
        // Not a raw number. Try parsing as a timestamp string.
        try {
          // Normalize format: replace space with 'T' (e.g., "2026-07-30 15:30:00Z" -> "2026-07-30T15:30:00Z")
          String normalized = strVal.replace(' ', 'T');
          Instant instant;
          if (normalized.endsWith("Z")) {
            instant = Instant.parse(normalized);
          } else {
            instant = OffsetDateTime.parse(normalized).toInstant();
          }
          seqHex = String.format("%016x", instant.toEpochMilli());
        } catch (Exception ex) {
          // If all parsing fails, fallback to raw character hex-encoding (legacy/fallback)
          seqHex = hexEncodeString(strVal);
        }
      }
    }

    if (seqHex != null) {
      // Append the Kafka offset as a second segment to break ties deterministically.
      // Both segments will be 16 characters (8 bytes) which is well within BigQuery's 32-character limit per segment.
      String offsetHex = String.format("%016x", record.kafkaOffset());
      return seqHex + "/" + offsetHex;
    }
    return null;
  }

  private String hexEncodeString(String strVal) {
    StringBuilder hexBuilder = new StringBuilder();
    for (char c : strVal.toCharArray()) {
      hexBuilder.append(String.format("%02x", (int) c));
    }
    return splitIntoSegments(hexBuilder.toString(), 32);
  }

  private String splitIntoSegments(String hexStr, int segmentSize) {
    StringBuilder result = new StringBuilder();
    int len = hexStr.length();
    for (int i = 0; i < len; i += segmentSize) {
      if (i > 0) {
        result.append("/");
      }
      result.append(hexStr.substring(i, Math.min(len, i + segmentSize)));
    }
    return result.toString();
  }

  public boolean isCdcEnabled() {
    boolean enabled = config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG) && config.isUpsertDeleteEnabled();
    logger.info("isCdcEnabled check - USE_STORAGE_WRITE_API: {}, isUpsertDeleteEnabled: {}, Result: {}",
        config.getBoolean(config.USE_STORAGE_WRITE_API_CONFIG), config.isUpsertDeleteEnabled(), enabled);
    return enabled;
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
}
