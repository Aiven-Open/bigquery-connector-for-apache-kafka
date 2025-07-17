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
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storage Write API writer that attempts to write all the rows it is given at once
 */
public class StorageWriteApiWriter implements Runnable {

  public static final String DEFAULT = "default";
  private final StorageWriteApiBase streamWriter;
  private final PartitionedTableId table;
  private final List<ConvertedRecord> records;
  private final String streamName;
  Logger logger = LoggerFactory.getLogger(StorageWriteApiWriter.class);

  /**
   * @param table        The table to write the records to
   * @param streamWriter The stream writer to use - Default, Batch etc
   * @param records      The records to write
   * @param streamName   The stream to use while writing data
   */
  public StorageWriteApiWriter(PartitionedTableId table, StorageWriteApiBase streamWriter, List<ConvertedRecord> records, String streamName) {
    this.streamWriter = streamWriter;
    this.records = records;
    this.table = table;
    this.streamName = streamName;
  }

  @Override
  public void run() {
    if (records.size() == 0) {
      logger.debug("There are no records, skipping");
      return;
    }
    logger.debug("Putting {} records into {} stream", records.size(), streamName);
    streamWriter.initializeAndWriteRecords(table, records, streamName);
  }

  public static class Builder implements TableWriterBuilder {
    private final List<ConvertedRecord> records = new ArrayList<>();
    private final SinkRecordConverter recordConverter;
    private final PartitionedTableId table;
    private final StorageWriteApiBase streamWriter;
    private final StorageApiBatchModeHandler batchModeHandler;

    public Builder(StorageWriteApiBase streamWriter,
                   PartitionedTableId table,
                   SinkRecordConverter recordConverter,
                   StorageApiBatchModeHandler batchModeHandler) {
      this.streamWriter = streamWriter;
      this.table = table;
      this.recordConverter = recordConverter;
      this.batchModeHandler = batchModeHandler;
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

    /**
     * Converts SinkRecord to JSONObject to be sent to BQ Streams
     *
     * @param record which is to be converted
     * @return converted record as JSONObject
     */
    private JSONObject convertRecord(SinkRecord record) {
      Map<String, Object> convertedRecord = recordConverter.getRegularRow(record);
      return getJsonFromMap(convertedRecord);
    }

    /**
     * @return Builds Storage write API writer which would do actual data ingestion using streams
     */
    @Override
    public Runnable build() {
      String streamName = DEFAULT;
      if (records.size() > 0 && streamWriter instanceof StorageWriteApiBatchApplicationStream) {
        TableName tableName = TableNameUtils.tableName(table.getBaseTableId());
        streamName = batchModeHandler.updateOffsetsOnStream(tableName.toString(), records);
      }
      return new StorageWriteApiWriter(table, streamWriter, records, streamName);
    }

    private JSONObject getJsonFromMap(Map<String, Object> map) {
      JSONObject jsonObject = new JSONObject();
      map.forEach((key, value) -> {
        if (value instanceof Map<?, ?>) {
          value = getJsonFromMap((Map<String, Object>) value);
        } else if (value instanceof List<?>) {
          JSONArray items = new JSONArray();
          ((List<?>) value).forEach(v -> {
            if (v instanceof Map<?, ?>) {
              items.put(getJsonFromMap((Map<String, Object>) v));
            } else {
              items.put(v);
            }
          });
          value = items;
        }
        jsonObject.put(key, value);
      });
      return jsonObject;
    }
  }
}
