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

package com.wepay.kafka.connect.bigquery.integration;

import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig.TIME_PARTITIONING_TYPE_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG;
import static com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TestCaseLogger;
import com.wepay.kafka.connect.bigquery.integration.utils.TimePartitioningTestUtils;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
@ExtendWith(TestCaseLogger.class)
class TimePartitioningIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(TimePartitioningIT.class);

  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 1;

  private BigQuery bigQuery;

  public static Stream<Arguments> testArguments() {
    int testCase = 0;
    return Stream.of(
        Arguments.of(TimePartitioning.Type.HOUR, false, false, testCase++),
        Arguments.of(TimePartitioning.Type.DAY, true, true, testCase++),
        Arguments.of(TimePartitioning.Type.DAY, true, false, testCase++),
        Arguments.of(TimePartitioning.Type.DAY, false, false, testCase++),
        Arguments.of(TimePartitioning.Type.MONTH, false, false, testCase++),
        Arguments.of(TimePartitioning.Type.YEAR, false, false, testCase));
  }

  @BeforeAll
  static void beforeAll() {
    startConnect();
  }

  @AfterAll
  static void afterAll() {
    stopConnect();
  }

  @BeforeEach
  void setup() {
    bigQuery = newBigQuery();
  }

  @AfterEach
  void teardown() {
    if (bigQuery != null) {
      delete(bigQuery, tableName());
    }
  }

  private Map<String, String> partitioningProps(
      TimePartitioning.Type partitioningType,
      boolean usePartitionDecorator,
      boolean messageTimePartitioning) {
    Map<String, String> result = new HashMap<>();

    // use the JSON converter with schemas enabled
    result.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    result.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());

    result.put(BIGQUERY_PARTITION_DECORATOR_CONFIG, Boolean.toString(usePartitionDecorator));
    result.put(
        BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG, Boolean.toString(messageTimePartitioning));
    result.put(TIME_PARTITIONING_TYPE_CONFIG, partitioningType.name());

    return result;
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  void testTimePartitioning(
      TimePartitioning.Type partitioningType,
      boolean usePartitionDecorator,
      boolean messageTimePartitioning,
      int testCase)
      throws Throwable {
    final long testStartTime = System.currentTimeMillis();

    // create topic in Kafka
    final String topic = topicName();
    assertCluster().kafka().createTopic(topic);

    // setup props for the sink connector
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(SinkConnectorConfig.TOPICS_CONFIG, topic);

    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");

    props.putAll(
        partitioningProps(partitioningType, usePartitionDecorator, messageTimePartitioning));

    // start a sink connector
    assertCluster().configureConnector(connectorName(), props);

    // wait for tasks to spin up
    waitForConnectorToStart(connectorName(), TASKS_MAX);

    // Instantiate the converter we'll use to send records to the connector
    Converter valueConverter = converter();

    TimePartitioningTestUtils.produceRecordsWithTimestamps(
        assertCluster(),
        topic,
        NUM_RECORDS_PRODUCED,
        testStartTime,
        partitioningType,
        valueConverter);

    // wait for tasks to write to BigQuery and commit offsets for their records
    waitForCommittedRecords(connectorName(), topic, NUM_RECORDS_PRODUCED, TASKS_MAX);

    TableName tableName = tableName();

    // Might fail to read from the table for a little bit; keep retrying until it's available
    waitForCondition(
        () -> {
          try {
            readAllRows(bigQuery, tableName, "i");
            return true;
          } catch (RuntimeException e) {
            logger.debug("Failed to read rows from table {}", tableName, e);
            return false;
          }
        },
        TimeUnit.MINUTES.toMillis(5),
        "Could not read from table to verify data after connector committed offsets for the expected number of records");

    List<List<Object>> allRows = readAllRows(bigQuery, tableName, "i");
    // Just check to make sure we sent the expected number of rows to the table. There can be
    // duplication so the check is at least there are NUM_RECORDS_PRODUCED
    assertTrue(NUM_RECORDS_PRODUCED <= allRows.size());

    // Ensure that the table was created with the expected time partitioning type
    StandardTableDefinition tableDefinition =
        bigQuery.getTable(TableNameUtils.tableId(tableName)).getDefinition();
    Optional<TimePartitioning.Type> actualPartitioningType =
        Optional.ofNullable((tableDefinition).getTimePartitioning()).map(TimePartitioning::getType);
    assertEquals(Optional.of(partitioningType), actualPartitioningType);

    // Verify that at least one record landed in each of the targeted partitions
    if (usePartitionDecorator && messageTimePartitioning) {
      for (int i = -1; i < 2; i++) {
        long partitionTime =
            TimePartitioningTestUtils.computeTimestamp(partitioningType, testStartTime, i);
        TimePartitioningTestUtils.assertPartitionContainsData(
            bigQuery, tableName, TimePartitioning.Type.DAY, partitionTime);
      }
    }
  }

  private Converter converter() {
    Map<String, Object> props = new HashMap<>();
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    Converter result = new JsonConverter();
    result.configure(props, false);
    return result;
  }
}
