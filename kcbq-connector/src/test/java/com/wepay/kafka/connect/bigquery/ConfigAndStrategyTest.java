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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.strategy.CommitStrategy;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiBase;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Observation-only reproduction harness. Nothing here asserts an expected outcome; each test prints
 * what the production code actually does so the result can be read from the surefire output.
 */
public class ConfigAndStrategyTest {
  private static final String TOPIC = "test_topic";

  private static void out(String s) {
    System.out.println("REPRO| " + s);
  }

  /** The commit strategy factory that the public no-arg (production) constructor installs. */
  @SuppressWarnings("unchecked")
  private static Function<BigQuerySinkTaskConfig, CommitStrategy> productionFactory()
      throws Exception {
    Field f = BigQuerySinkTask.class.getDeclaredField("commitStrategyFactory");
    f.setAccessible(true);
    return (Function<BigQuerySinkTaskConfig, CommitStrategy>) f.get(new BigQuerySinkTask());
  }

  private static Map<String, String> baseProps() {
    Map<String, String> p = new SinkTaskPropertiesFactory().getProperties();
    p.put(BigQuerySinkConfig.TOPICS_CONFIG, TOPIC);
    p.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    p.remove("useStorageWriteApi");
    return p;
  }

  private static SinkRecord rec(long offset) {
    Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", "k" + offset);
    Schema valueSchema = SchemaBuilder.struct().field("f", Schema.STRING_SCHEMA).build();
    Struct value = new Struct(valueSchema).put("f", "v" + offset);
    return new SinkRecord(
        TOPIC, 0, keySchema, key, valueSchema, value, offset, 0L, TimestampType.CREATE_TIME);
  }

  private Map<String, Object> expectedConfig() {
    Map<String, Object> expected = new HashMap<>();
    expected.put("isUpsertDeleteEnabled()", false);
    expected.put("productionCommitStrategy", "HighestUnblockedCommitStrategy");
    expected.put("upsertDelete()", false);
    expected.put("useStorageApi()", false);
    expected.put("useStorageApiBatchMode()", false);
    expected.put("useStorageWriteApi()", false);
    expected.put("enableBatchMode()", false);
    return expected;
  }

  @ParameterizedTest
  @MethodSource("matrixData")
  void matrixTest(String name, Map<String, String> extraProps, Map<String, Object> values)
      throws Exception {
    Function<BigQuerySinkTaskConfig, CommitStrategy> factory = productionFactory();
    Map<String, String> p = baseProps();
    p.putAll(extraProps);

    Map<String, Object> expected = expectedConfig();
    expected.putAll(values);

    BigQuerySinkTaskConfig cfg = new BigQuerySinkTaskConfig(p);
    Map<String, Object> actual = new HashMap<>();
    actual.put("useStorageWriteApi()", cfg.useStorageWriteApi());
    actual.put("useStorageApi()", cfg.useStorageApi());
    actual.put("useStorageApiBatchMode()", cfg.useStorageApiBatchMode());
    actual.put("upsertDelete()", cfg.upsertDelete());
    actual.put("isUpsertDeleteEnabled()", cfg.isUpsertDeleteEnabled());
    actual.put("productionCommitStrategy", factory.apply(cfg).getClass().getSimpleName());
    actual.put("enableBatchMode()", cfg.enableBatchMode());

    assertThat(actual).containsExactlyInAnyOrderEntriesOf(expected);
  }

  static List<Arguments> matrixData() {
    List<Arguments> lst = new ArrayList<>();
    lst.add(Arguments.of("insertAll (default)", Collections.emptyMap(), Collections.emptyMap()));

    lst.add(
        Arguments.of(
            "useStorageWriteApi=true",
            Map.of("useStorageWriteApi", "true"),
            Map.of("useStorageWriteApi()", true, "useStorageApi()", true)));
    lst.add(
        Arguments.of(
            "useStorageWriteApi=true+enableBatchMode=true",
            Map.of("useStorageWriteApi", "true", "enableBatchMode", "true"),
            Map.of(
                "useStorageWriteApi()",
                true,
                "useStorageApi()",
                true,
                "enableBatchMode()",
                true,
                "useStorageApiBatchMode()",
                true)));
    lst.add(
        Arguments.of(
            "isCdcEnabled=true alone",
            Map.of("isCdcEnabled", "true"),
            Map.of("useStorageWriteApi()", true, "isUpsertDeleteEnabled()", true)));

    lst.add(
        Arguments.of(
            "configPreset=debezium_cdc alone",
            Map.of("configPreset", "debezium_cdc"),
            Map.of("useStorageWriteApi()", true, "isUpsertDeleteEnabled()", true)));
    lst.add(
        Arguments.of(
            "isCdcEnabled=true+useStorageWriteApi=true",
            Map.of("isCdcEnabled", "true", "useStorageWriteApi", "true"),
            Map.of(
                "useStorageWriteApi()", true,
                "useStorageApi()", true,
                "isUpsertDeleteEnabled()", true)));
    lst.add(
        Arguments.of(
            "isCdcEnabled=true+upsertEnabled=true",
            Map.of("isCdcEnabled", "true", "upsertEnabled", "true"),
            Map.of(
                "useStorageWriteApi()", true,
                "isUpsertDeleteEnabled()", true,
                "upsertDelete()", true)));
    lst.add(
        Arguments.of(
            "isCdcEnabled=true+upsertEnabled=true+deleteEnabled=true",
            Map.of("isCdcEnabled", "true", "upsertEnabled", "true", "deleteEnabled", "true"),
            Map.of(
                "useStorageWriteApi()", true,
                "isUpsertDeleteEnabled()", true,
                "upsertDelete()", true)));
    lst.add(
        Arguments.of(
            "isCdcEnabled=true+enableBatchMode=true",
            Map.of("isCdcEnabled", "true", "enableBatchMode", "true"),
            Map.of(
                "useStorageWriteApi()",
                true,
                "isUpsertDeleteEnabled()",
                true,
                "enableBatchMode()",
                true)));
    lst.add(
        Arguments.of(
            "legacy upsertEnabled=true+kafkaKeyFieldName",
            Map.of("upsertEnabled", "true", "kafkaKeyFieldName", "kk", "mergeIntervalMs", "60000"),
            Map.of(
                "isUpsertDeleteEnabled()", true,
                "upsertDelete()", true)));

    return lst;
  }

  // ---------------------------------------------------------------- task-level harness
  private static class Harness {
    final CountDownLatch writeStarted = new CountDownLatch(1);
    final StorageWriteApiDefaultStream stream =
        mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    final BigQuery bigQuery = mock(BigQuery.class);
    BigQuerySinkTask task;

    Harness() throws Exception {
      doAnswer(
              inv -> {
                StorageWriteApiBase base = (StorageWriteApiBase) inv.getMock();

                writeStarted.countDown();
                return null;
              })
          .when(stream)
          .initializeAndWriteRecords(
              any(PartitionedTableId.class),
              anyList(),
              eq(DEFAULT),
              any(SinkRecordConverter.class),
              any());
      doNothing().when(stream).shutdown();
      Table table = mock(Table.class);
      when(bigQuery.getTable(any(TableId.class))).thenReturn(table);
      InsertAllResponse resp = mock(InsertAllResponse.class);
      when(resp.hasErrors()).thenReturn(false);
      when(bigQuery.insertAll(any()))
          .thenAnswer(
              inv -> {
                writeStarted.countDown();
                return resp;
              });
      task =
          new BigQuerySinkTask(
              bigQuery,
              mock(Storage.class),
              mock(SchemaManager.class),
              stream,
              mock(StorageApiBatchModeHandler.class),
              new MockTime(),
              productionFactory());
      task.initialize(mock(SinkTaskContext.class));
    }
  }

  private static String commitStrategyOf(BigQuerySinkTask task) throws Exception {
    Field f = BigQuerySinkTask.class.getDeclaredField("commitStrategy");
    f.setAccessible(true);
    return f.get(task).getClass().getSimpleName();
  }

  private static void runScenario(String name, Map<String, String> props) {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    try {
      Harness h = new Harness();
      h.task.start(props);
      h.task.open(Collections.singleton(tp));
      out("SCENARIO [" + name + "] started; commitStrategy=" + commitStrategyOf(h.task));
      // Kafka Connect hands preCommit "last consumed offset + 1".
      Map<TopicPartition, OffsetAndMetadata> connectOffsets =
          Collections.singletonMap(tp, new OffsetAndMetadata(3));
      h.task.put(List.of(rec(0), rec(1), rec(2)));
      boolean started = h.writeStarted.await(5, TimeUnit.SECONDS);
      out("SCENARIO [" + name + "] put(offsets 0..2) returned; backend write started=" + started);
      //      h.writeDone.await(5, TimeUnit.SECONDS);

      Awaitility.waitAtMost(Duration.ofMinutes(5))
          .pollInterval(Duration.ofMillis(500))
          .untilAsserted(
              () -> {
                System.out.println(h.task.getExecutor().taskNames());
                System.out.println(h.task.getScheduledExecutor().taskNames());
                assertThat(h.task.preCommit(Map.of(tp, new OffsetAndMetadata(10))))
                    .containsExactlyEntriesOf(connectOffsets);
              });
      // try {
      h.task.put(Collections.emptyList());
      out("SCENARIO [" + name + "] follow-up put(empty) OK");
      h.task.stop();
    } catch (Throwable t) {
      fail("SCENARIO [" + name + "] FAILED TO RUN: " + t, t);
    }
  }

  @Test
  void StorageWriteApiDefaultStreamTest() {
    Map<String, String> p = baseProps();
    p.put("useStorageWriteApi", "true");
    runScenario("R2 useStorageWriteApi=true default stream", p);
  }

  @Test
  void InsertAllDefaultPathTest() {
    Map<String, String> p = baseProps();
    runScenario("R4 insertAll default path", p);
  }

  /** insertAll path where the newest records of the partition are tombstones (skipped). */
  @Test
  void InsertAllTrailingTombstonesTest() throws Exception {
    String name = "R4b insertAll, offsets 0-1 data + offsets 2-3 tombstones";
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Harness h = new Harness();
    h.task.start(baseProps());
    h.task.open(Collections.singleton(tp));
    SinkRecord t2 =
        new SinkRecord(TOPIC, 0, null, null, null, null, 2, 0L, TimestampType.CREATE_TIME);
    SinkRecord t3 =
        new SinkRecord(TOPIC, 0, null, null, null, null, 3, 0L, TimestampType.CREATE_TIME);
    h.task.put(List.of(rec(0), rec(1), t2, t3));
    Map<TopicPartition, OffsetAndMetadata> expected = Map.of(tp, new OffsetAndMetadata(4));
    Awaitility.waitAtMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () ->
                assertThat(h.task.preCommit(Map.of(tp, new OffsetAndMetadata(10))))
                    .containsExactlyEntriesOf(expected));
    out(
        "SCENARIO ["
            + name
            + "] Connect proposes offset 4; preCommit returned "
            + h.task.preCommit(Collections.singletonMap(tp, new OffsetAndMetadata(4))));
    // A poll that contains only tombstones.
    SinkRecord t4 =
        new SinkRecord(TOPIC, 0, null, null, null, null, 4, 0L, TimestampType.CREATE_TIME);
    h.task.put(List.of(t4));
    Thread.sleep(300);
    out(
        "SCENARIO ["
            + name
            + "] after a tombstone-only poll (offset 4), Connect proposes 5; preCommit returned "
            + h.task.preCommit(Collections.singletonMap(tp, new OffsetAndMetadata(5))));
    h.task.stop();
  }

  @Test
  void CdcAloneTest() {
    Map<String, String> p = baseProps();
    p.put("isCdcEnabled", "true");
    runScenario("R3a isCdcEnabled=true alone", p);
  }

  @Test
  void CdcPresetAloneTest() {
    Map<String, String> p = baseProps();
    p.put("configPreset", "debezium_cdc");
    runScenario("R3a configPreset=debezium_cdc alone", p);
  }

  @Test
  void CdcExplicitStorageApiTest() {
    Map<String, String> p = baseProps();
    p.put("isCdcEnabled", "true");
    p.put("useStorageWriteApi", "true");
    runScenario("R3c isCdcEnabled=true + useStorageWriteApi=true", p);
  }

  @Test
  void CdcPlusExplicitUpsertTest() {
    Map<String, String> p = baseProps();
    p.put("isCdcEnabled", "true");
    p.put("upsertEnabled", "true");
    runScenario("R3b isCdcEnabled=true + upsertEnabled=true", p);
  }
}
