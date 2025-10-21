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

import static com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiWriter.DEFAULT;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BigQueryStorageApiSinkTaskTest {
  private static final SinkTaskPropertiesFactory propertiesFactory = new SinkTaskPropertiesFactory();
  final String topic = "test_topic";
  private final AtomicLong spoofedRecordOffset = new AtomicLong();
  private final StorageWriteApiDefaultStream mockedStorageWriteApiDefaultStream = mock(
      StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
  ;
  Map<String, String> properties;
  BigQuery bigQuery = mock(BigQuery.class);

  Storage storage = mock(Storage.class);
  SinkTaskContext sinkTaskContext = mock(SinkTaskContext.class);
  SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
  SchemaManager schemaManager = mock(SchemaManager.class);
  Time time = new MockTime();
  StorageApiBatchModeHandler storageApiBatchHandler = mock(StorageApiBatchModeHandler.class);
  BigQuerySinkTask testTask = new BigQuerySinkTask(
      bigQuery, schemaRetriever, storage, schemaManager, mockedStorageWriteApiDefaultStream, storageApiBatchHandler, time);

  @BeforeEach
  public void setUp() {
    spoofedRecordOffset.set(0);
    properties = propertiesFactory.getProperties();

    properties.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    properties.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    spoofedRecordOffset.set(0);

    doNothing().when(mockedStorageWriteApiDefaultStream)
            .initializeAndWriteRecords(any(PartitionedTableId.class), anyList(), eq(DEFAULT));
    doNothing().when(mockedStorageWriteApiDefaultStream).shutdown();

    testTask.initialize(sinkTaskContext);
    testTask.start(properties);
  }

  @Test
  public void testPut() {
    testTask.put(Collections.singletonList(spoofSinkRecord()));
    testTask.flush(Collections.emptyMap());

    verify(mockedStorageWriteApiDefaultStream, times(1))
            .initializeAndWriteRecords(any(PartitionedTableId.class), anyList(), eq(DEFAULT));
  }

  @Test
  public void testSimplePutException() {
    BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("error 12345");

    doThrow(exception).when(mockedStorageWriteApiDefaultStream)
            .initializeAndWriteRecords(any(PartitionedTableId.class), anyList(), eq(DEFAULT));

    testTask.put(Collections.singletonList(spoofSinkRecord()));
    BigQueryConnectException e = assertThrows(
        BigQueryConnectException.class,
        () -> {
          // Try for at most 1 second to get the task to throw an exception
          for (long startTime = System.currentTimeMillis(); System.currentTimeMillis() < startTime + 1_000; Thread.sleep(100)) {
            testTask.put(Collections.emptyList());
          }
        }
    );
    assertInstanceOf(BigQueryStorageWriteApiConnectException.class, e.getCause());
  }

  @Test
  public void testStop() {
    testTask.stop();

    verify(mockedStorageWriteApiDefaultStream, times(1)).shutdown();

    assertThrows(
        RejectedExecutionException.class,
        () -> testTask.put(Collections.singletonList(spoofSinkRecord()))
    );
  }

  private SinkRecord spoofSinkRecord() {

    Schema basicValueSchema = SchemaBuilder
        .struct()
        .field("sink_task_test_field", Schema.STRING_SCHEMA)
        .build();
    Struct basicValue = new Struct(basicValueSchema);
    basicValue.put("sink_task_test_field", "sink task test row");


    return new SinkRecord(topic, 0, null, null,
        basicValueSchema, basicValue, spoofedRecordOffset.getAndIncrement(), null, TimestampType.NO_TIMESTAMP_TYPE);
  }
}
