/*
 * Copyright 2026 Aiven Oy and
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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.rpc.Code;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import de.huxhorn.sulky.ulid.ULID;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StorageWriteApiBaseTest {

    private static final ULID ULID_GENERATOR = new ULID();

    private List<ConvertedRecord> mmkConvertedRecords(int count) {
        List<ConvertedRecord> convertedRecords = new ArrayList<>();

        Schema valueSchema = SchemaBuilder.struct()
                .field("value", Schema.STRING_SCHEMA);


        for (int i = 0; i < count; i++) {
            Struct value = new Struct(valueSchema);
            value.put("value", "value" + i);
            SinkRecord sinkRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key " + i,
                    valueSchema,value, (long)i);

            convertedRecords.add(new ConvertedRecord(sinkRecord, mock(JSONObject.class)));
        }
        return convertedRecords;
    }

    private Map<String, String> defaultMap = Map.of("project", "project", "defaultDataset", "defaultDataset",
            "kafkaDataFieldName", "kafkaDataFieldName", "taskId", "1");

    private void executeInitializeAndWriteRecordsTestData(TestingStreamWriter streamWriter, BigQuerySinkTaskConfig config, SchemaManager schemaManager) {
        SinkRecordConverter recordConverter = new SinkRecordConverter(config, null, null);

        BigQueryWriteClient bigQueryWriteClient = mock(BigQueryWriteClient.class);
        int retry = 5;
        long retryWait = 5000;
        BigQueryWriteSettings writeSettings = mock(BigQueryWriteSettings.class);
        boolean autoCreateTables = true;
        ErrantRecordHandler errantRecordHandler = null;
        boolean allowNewBigQueryFields = config.getBoolean(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG);
        boolean allowRequiredFieldRelaxation = config.getBoolean(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG);
        boolean attemptSchemaUpdate = allowNewBigQueryFields || allowRequiredFieldRelaxation;

        // create an instance
        StorageWriteApiBase underTest = new StorageWriteApiBase(retry, retryWait, writeSettings, autoCreateTables, errantRecordHandler,
                schemaManager, attemptSchemaUpdate, config) {

            @Override
            public void preShutdown() {

            }

            @Override
            protected StreamWriter streamWriter(PartitionedTableId table, String streamName, List<ConvertedRecord> records) {
                return streamWriter;
            }

            @Override
            protected BigQueryWriteClient getWriteClient() {
                return bigQueryWriteClient;
            }
        };
        PartitionedTableId partitionTableId = new PartitionedTableId.Builder("dataset", "baseTable").setPartition("partition")
                .setProject("project").build();
        List<ConvertedRecord> rows = mmkConvertedRecords(5);
        String streamName = "initializeAndWriteRecordsTest";
        Supplier<String> ulidSupplier = ULID_GENERATOR::nextULID;

        underTest.initializeAndWriteRecords(partitionTableId, rows, streamName, recordConverter, ulidSupplier);
    }

    @Test
    void appendRowsTooLargeTestWithStorageWriteAPI() {
        RowsTooLargeStreamWriter streamWriter = new RowsTooLargeStreamWriter("appendRowsTooLargeTestWithStorageWriteAPI");
        BigQuerySinkTaskConfig config = storageWriteApiConfig();
        executeInitializeAndWriteRecordsTestData(streamWriter, config, null);

        // verify the results
        List<JSONArray> result = streamWriter.appendCalls;
        Set<String> ulid = new LinkedHashSet<>();
        // 1 failure, 2 groups of 2, 1 gorup of 1 = 4 attempts
        assertEquals(4, result.size());
        Set<String> ulids = new HashSet<>();
        // we should have 2 ulids.  The one from the first set that failed
        // and the one from the 2nd set (3 entries) that succeeded.
        for (int i = 0; i < result.size(); i++) {
            ulids.add(assertSameUlid(result.get(i)));
        }
        assertEquals(4, ulids.size(), ulids::toString);
    }

    @Test
    void appendRowsTooLargeTestWithStorageWriteGCS() {
        RowsTooLargeStreamWriter streamWriter = new RowsTooLargeStreamWriter("appendRowsTooLargeTestWithStorageWriteGCS");
        BigQuerySinkTaskConfig config = storageWriteGCSConfig();
        executeInitializeAndWriteRecordsTestData(streamWriter, config, null);

        // verify the results
        List<JSONArray> result = streamWriter.appendCalls;
        Set<String> ulid = new LinkedHashSet<>();
        // 1 failure, 2 groups of 2, 1 gorup of 1 = 4 attempts
        assertEquals(4, result.size());
        Set<String> ulids = new HashSet<>();
        // we should have 2 ulids.  The one from the first set that failed
        // and the one from the 2nd set (3 entries) that succeeded.
        for (int i = 0; i < result.size(); i++) {
            ulids.add(assertSameUlid(result.get(i)));
        }
        assertEquals(4, ulids.size(), ulids::toString);
    }

    @Test
    void updateSchemaTestWithStorageWriteAPI() {

        BigQuerySinkTaskConfig config = storageWriteApiConfig();

        UpdatedSchemaStreamWriter streamWriter = new UpdatedSchemaStreamWriter("updateSchemaTestWithStorageWriteAPI");

        SchemaManager schemaManager = mock(SchemaManager.class);
        executeInitializeAndWriteRecordsTestData(streamWriter, config,  schemaManager);

        verify(schemaManager, times(1)).updateSchema(any(TableId.class), any(List.class));

        // verify the results
;        List<JSONArray> result = streamWriter.appendCalls;
        Set<String> ulid = new LinkedHashSet<>();
        // 1 failure (schema change), 1 success = 2 attempts
        assertEquals(2, result.size());
        // we should have 2 ulids.  The one from the first set that failed
        // and the one from the 2nd set (3 entries) that succeeded.
        String ulid1 = assertSameUlid(result.get(0));
        String ulid2 = assertSameUlid(result.get(1));
        assertNotEquals(ulid2, ulid1);
    }

    @Test
    void updateSchemaTestWithStorageWriteGCS() {

        BigQuerySinkTaskConfig config = storageWriteGCSConfig();

        UpdatedSchemaStreamWriter streamWriter = new UpdatedSchemaStreamWriter("updateSchemaTestWithStorageWriteGCS");

        SchemaManager schemaManager = mock(SchemaManager.class);
        executeInitializeAndWriteRecordsTestData(streamWriter, config,  schemaManager);

        verify(schemaManager, times(1)).updateSchema(any(TableId.class), any(List.class));

        // verify the results
        ;        List<JSONArray> result = streamWriter.appendCalls;
        Set<String> ulid = new LinkedHashSet<>();
        // 1 failure (schema change), 1 success = 2 attempts
        assertEquals(2, result.size());
        // we should have 2 ulids.  The one from the first set that failed
        // and the one from the 2nd set (3 entries) that succeeded.
        String ulid1 = assertSameUlid(result.get(0));
        String ulid2 = assertSameUlid(result.get(1));
        assertNotEquals(ulid2, ulid1);
    }

    private String assertSameUlid(JSONArray ary) {
        Set<String> ulids = new HashSet<>();
        for (int i=0; i<ary.length(); i++) {
            ulids.add(ary.getJSONObject(i).getJSONObject("kafkaDataFieldName").getString("putAttemptId"));
        }
        assertEquals(1, ulids.size(), "more than one ULID in JSONArray");
        return ulids.iterator().next();
    }

    BigQuerySinkTaskConfig storageWriteApiConfig() {
        Map<String, String> params = new HashMap<>(defaultMap);
        params.put("useStorageWriteApi", "true");
        params.put("trackPutAttempts", "true");
        params.put("allowNewBigQueryFields", "true");
        return new BigQuerySinkTaskConfig(params);
    }

    BigQuerySinkTaskConfig storageWriteGCSConfig() {
        Map<String, String> params = new HashMap<>(defaultMap);
        params.put("useStorageWriteApi", "false");
        params.put("trackPutAttempts", "true");
        params.put("allowNewBigQueryFields", "true");
        return new BigQuerySinkTaskConfig(params);
    }

    /**
     * A StreamWriter implementation that simply captures the rows sent in the appendRows() method and creates
     * a future to simulate Storage activity.
     */
    private abstract static class TestingStreamWriter implements StreamWriter {
        final String name;
        final List<JSONArray> appendCalls = new ArrayList<>();
        int successCount;
        int refreshCount;

        TestingStreamWriter(String name) {
            this.name = name;
        }

        abstract ApiFuture<AppendRowsResponse> createFuture();

        @Override
        final public String streamName() {
            return name;
        }
        @Override
        public String toString() {
            return name;
        }
        @Override
        public ApiFuture<AppendRowsResponse> appendRows(JSONArray rows)  {
            appendCalls.add(rows);
            return createFuture();
        }

        @Override
        public void refresh() {
            refreshCount++;
        }

        @Override
        public void onSuccess() {
            successCount++;
        }

    };

    static class RowsTooLargeStreamWriter extends TestingStreamWriter {

        RowsTooLargeStreamWriter(String name) {
            super(name);
        }

        // createFuture implementation that fails the first write with a "request too large" error
        // and then accepts the rest.
        @Override
        ApiFuture<AppendRowsResponse> createFuture() {

            ApiFuture<AppendRowsResponse> future = mock(ApiFuture.class);
            try {
                if (appendCalls.size() == 1) {
                    Exceptions.AppendSerializtionError err = new Exceptions.AppendSerializtionError(
                            Code.INVALID_ARGUMENT.getNumber(),
                            "AppendRows request too large",
                            "appendRowsTooLargeTest",
                            Collections.emptyMap());

                    when(future.get()).thenThrow(err);
                } else {
                    AppendRowsResponse response = mock(AppendRowsResponse.class);
                    when(future.get()).thenReturn(response);
                    when(response.hasUpdatedSchema()).thenReturn(false);
                    when(response.hasError()).thenReturn(false);
                    when(response.hasAppendResult()).thenReturn(true);
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            return future;
        }
    }

    static class UpdatedSchemaStreamWriter extends TestingStreamWriter {

        UpdatedSchemaStreamWriter(String name) {
            super(name);
        }

        // createFuture implementation that fails the first write with a "request too large" error
        // and then accepts the rest.
        @Override
        ApiFuture<AppendRowsResponse> createFuture() {

            ApiFuture<AppendRowsResponse> future = mock(ApiFuture.class);
            try {
                if (appendCalls.size() == 1) {
                    AppendRowsResponse response = mock(AppendRowsResponse.class);
                    when(future.get()).thenReturn(response);
                    when(response.hasUpdatedSchema()).thenReturn(true);
                    when(response.hasError()).thenReturn(false);
                    when(response.hasAppendResult()).thenReturn(true); // does this matter?
                } else {
                    AppendRowsResponse response = mock(AppendRowsResponse.class);
                    when(future.get()).thenReturn(response);
                    when(response.hasUpdatedSchema()).thenReturn(false);
                    when(response.hasError()).thenReturn(false);
                    when(response.hasAppendResult()).thenReturn(true);
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            return future;
        }
    }
}
