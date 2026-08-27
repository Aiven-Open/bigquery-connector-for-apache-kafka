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

import static com.google.cloud.bigquery.LegacySQLTypeName.BIGNUMERIC;
import static com.google.cloud.bigquery.LegacySQLTypeName.BOOLEAN;
import static com.google.cloud.bigquery.LegacySQLTypeName.BYTES;
import static com.google.cloud.bigquery.LegacySQLTypeName.DATE;
import static com.google.cloud.bigquery.LegacySQLTypeName.DATETIME;
import static com.google.cloud.bigquery.LegacySQLTypeName.FLOAT;
import static com.google.cloud.bigquery.LegacySQLTypeName.INTEGER;
import static com.google.cloud.bigquery.LegacySQLTypeName.NUMERIC;
import static com.google.cloud.bigquery.LegacySQLTypeName.STRING;
import static com.google.cloud.bigquery.LegacySQLTypeName.TIME;
import static com.google.cloud.bigquery.LegacySQLTypeName.TIMESTAMP;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.GcpClientBuilder;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TestCaseLogger;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import de.huxhorn.sulky.ulid.ULID;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.NoRetryException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("integration")
@ExtendWith(TestCaseLogger.class)
public abstract class BaseConnectorIT {
  protected static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
  protected static final long COMMIT_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(5);
  protected static final long OFFSETS_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
  protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  private static final Logger logger = LoggerFactory.getLogger(BaseConnectorIT.class);
  private static final String KEY_SOURCE_ENV_VAR = "KCBQ_TEST_KEY_SOURCE";
  private static final String KEYFILE_ENV_VAR = "KCBQ_TEST_KEYFILE";
  private static final String PROJECT_ENV_VAR = "KCBQ_TEST_PROJECT";
  private static final String DATASET_ENV_VAR = "KCBQ_TEST_DATASET";
  private static final String GCS_BUCKET_ENV_VAR = "KCBQ_TEST_BUCKET";
  private static final String GCS_FOLDER_ENV_VAR = "KCBQ_TEST_FOLDER";
  private static final String TEST_NAMESPACE_ENV_VAR = "KCBQ_TEST_TABLE_SUFFIX";
  protected static final long ONE_MINUTE = 60_000L;
  protected static final long ONE_SECOND = 1_000L;

  /** ULID for naming resolution */
  private final ULID ulid = new ULID();
  /** The default suffixes for this instance */
  private final String defaultSuffix = ulid.nextULID();
  /** THe test info for currently executed tests. */
  private TestInfo testInfo;

  /** The mbedded cluster for running Kafka connect */
  protected EmbeddedConnectCluster connect;

  /** The status message if there are any issues with the connector status check */
  protected String connectorStatus;

  private Admin kafkaAdminClient;

  /**
   * Converts byte[] to Byte[]
   * @param bytes the bytes for the array.
   * @return a Byte[] that contains the bytes from {@code bytes}
   */
  protected static List<Byte> boxByteArray(byte[] bytes) {
    Byte[] result = new Byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = bytes[i];
    }
    return Arrays.asList(result);
  }

  @BeforeEach
  void BaseConnectorSetup(TestInfo testInfo) {
    this.testInfo = testInfo;
  }

  /**
   * Gets the topic name for the test.
   * @return the topic name for the test.
   */
  protected final String topicName() {
    final String[] names = new String[3] ;
    testInfo.getTestClass().ifPresent( c -> names[0] = c.getSimpleName());
    names[1] = testInfo.getDisplayName().replaceAll("\\(\\)", "");
    testInfo.getTestMethod().ifPresent( m -> names[1] = m.getName());
    names[2] = tableSuffix();
    return String.join("_",names);
  }

  /**
   * Gets the table name for the test.
   * @return the table name for the test.
   */
  protected final TableName tableName() {
    return TableName.of(project(), dataset(), FieldNameSanitizer.sanitizeName(topicName()));
  }

  /**
   * Convenience method to delete the table from bigquery.
   * @param bigQuery The big query instance to delete from.
   * @param tableName the table name  to delete.
   */
  protected final void delete(BigQuery bigQuery, TableName tableName) {
    bigQuery.delete(TableId.of(tableName.getDataset(), tableName.getProject(), tableName.getTable()));
  }

  /**
   * Starts the embedded connect cluster.
   */
  protected void startConnect() {
    Map<String, String> workerProps = new HashMap<>();
    workerProps.put(
        WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, Long.toString(OFFSET_COMMIT_INTERVAL_MS));
    // Allow per-connector consumer configuration for throughput testing
    workerProps.put(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG, "All");
    // Some external plugin dependencies don't yet have service loader manifests
    workerProps.put(WorkerConfig.PLUGIN_DISCOVERY_CONFIG, "HYBRID_WARN");

    Properties brokerProps = new Properties();
    brokerProps.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, 10 * 1024 * 1024);

    connect =
        new EmbeddedConnectCluster.Builder()
            .name("kcbq-connect-cluster")
            .numBrokers(1)
            .brokerProps(brokerProps)
            .workerProps(workerProps)
            .build();

    // start the clusters
    connect.start();

    kafkaAdminClient = connect.kafka().createAdminClient();
  }

  /**
   * Stops the embedded connect cluster.
   */
  protected final void stopConnect() {
    if (kafkaAdminClient != null) {
      Utils.closeQuietly(kafkaAdminClient, "admin client for embedded Kafka cluster");
      kafkaAdminClient = null;
    }

    // stop all Connect, Kafka and Zk threads.
    if (connect != null) {
      Utils.closeQuietly(connect::stop, "embedded Connect, Kafka, and Zookeeper clusters");
      connect = null;
    }
  }

  protected Map<String, String> baseConnectorProps(int tasksMax) {
    Map<String, String> result = new HashMap<>();

    result.put(CONNECTOR_CLASS_CONFIG, "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector");
    result.put(TASKS_MAX_CONFIG, Integer.toString(tasksMax));

    result.put(BigQuerySinkConfig.PROJECT_CONFIG, project());
    result.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, dataset());
    result.put(BigQuerySinkConfig.KEYFILE_CONFIG, keyFile());
    result.put(BigQuerySinkConfig.KEY_SOURCE_CONFIG, keySource());

    result.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    result.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");
    return result;
  }

  /**
   * Creates a new BigQuery instance.
   * Uses the credentials and options from the environment variables.
   * @return a new BigQuery instance.
   */
  protected BigQuery newBigQuery() {
    try {
      return new GcpClientBuilder.BigQueryBuilder()
          .withKey(keyFile())
          .withKeySource(GcpClientBuilder.KeySource.valueOf(keySource()))
          .withProject(project())
          .build();
    } catch (RuntimeException e) {
      LoggerFactory.getLogger(BaseConnectorIT.class).error("query error", e);
      throw e;
    }
  }

  protected void waitForCommittedRecords(
      String connector, String topic, long numRecords, int numTasks) throws InterruptedException {
    waitForCommittedRecords(
        connector, Collections.singleton(topic), numRecords, numTasks, COMMIT_MAX_DURATION_MS);
  }

  protected void waitForCommittedRecords(
      String connector, Collection<String> topics, long numRecords, int numTasks, long timeoutMs)
      throws InterruptedException {
    waitForCondition(
        () -> {
          long totalCommittedRecords = totalCommittedRecords(connector, topics);
          if (totalCommittedRecords >= numRecords) {
            logger.debug(
                "Connector has successfully committed {} records for topics {}",
                totalCommittedRecords,
                topics);
            return true;
          } else {
            // Check to make sure the connector is still running. If not, fail fast
            try {
              assertTrue(
                  assertConnectorAndTasksRunning(connector, numTasks).orElse(false),
                  () -> "Connector or one of its tasks failed during testing: " + connectorStatus);
            } catch (AssertionError e) {
              throw new NoRetryException(e);
            }
            logger.debug(
                "Connector has only committed {} records for topics {} so far; {} expected",
                totalCommittedRecords,
                topics,
                numRecords);
            // Sleep here so as not to spam Kafka with list-offsets requests
            Thread.sleep(OFFSET_COMMIT_INTERVAL_MS / 2);
            return false;
          }
        },
        timeoutMs,
        "Either the connector failed, or the message commit duration expired without all expected messages committed");
  }

  protected synchronized long totalCommittedRecords(String connector, Collection<String> topics)
      throws TimeoutException, ExecutionException, InterruptedException {
    // See
    // https://github.com/apache/kafka/blob/f7c38d83c727310f4b0678886ba410ae2fae9379/connect/runtime/src/main/java/org/apache/kafka/connect/util/SinkUtils.java
    // for how the consumer group ID is constructed for sink connectors
    Map<TopicPartition, OffsetAndMetadata> offsets =
        kafkaAdminClient
            .listConsumerGroupOffsets("connect-" + connector)
            .partitionsToOffsetAndMetadata()
            .get(OFFSETS_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);

    logger.trace("Connector {} has so far committed offsets {}", connector, offsets);

    return offsets.entrySet().stream()
        .filter(entry -> topics.contains(entry.getKey().topic()))
        .mapToLong(entry -> entry.getValue().offset())
        .sum();
  }

  /**
   * Read all rows from the given table.
   *
   * @param bigQuery used to connect to BigQuery
   * @param tableName the table to read
   * @param sortColumn a column to sort rows by (can use dot notation to refer to nested fields)
   * @return a list of all rows from the table, in random order.
   */
  @Deprecated
  protected List<List<Object>> readAllRows(BigQuery bigQuery, String tableName, String sortColumn)
          throws InterruptedException {

    final Table table = bigQuery.getTable(dataset(), tableName);
    final Schema schema = table.getDefinition().getSchema();

    TableResult tableResult =
            bigQuery.query(
                    QueryJobConfiguration.of(
                            String.format(
                                    "SELECT * FROM `%s`.`%s` ORDER BY %s ASC", dataset(), tableName, sortColumn)));

    return StreamSupport.stream(tableResult.iterateAll().spliterator(), false)
            .map(fieldValues -> convertRow(schema.getFields(), fieldValues))
            .collect(Collectors.toList());
  }

  /**
   * Read all rows from the given table.
   *
   * @param bigQuery used to connect to BigQuery
   * @param tableName the table to read
   * @param sortColumn a column to sort rows by (can use dot notation to refer to nested fields)
   * @return a list of all rows from the table, in random order.
   */
  protected final List<List<Object>> readAllRows(final BigQuery bigQuery, final TableName tableName, final String sortColumn)
      throws InterruptedException {

    final Table table = bigQuery.getTable(tableName.getDataset(), tableName.getTable());
    final Schema schema = table.getDefinition().getSchema();

    TableResult tableResult =
        bigQuery.query(
            QueryJobConfiguration.of(
                String.format(
                    "SELECT * FROM `%s`.`%s` ORDER BY %s ASC", tableName.getDataset(), tableName.getTable(), sortColumn)));

    return StreamSupport.stream(tableResult.iterateAll().spliterator(), false)
        .map(fieldValues -> convertRow(schema.getFields(), fieldValues))
        .collect(Collectors.toList());
  }

  protected long countRows(BigQuery bigQuery, String tableName) throws InterruptedException {
    TableResult tableResult =
        bigQuery.query(
            QueryJobConfiguration.of(
                "SELECT COUNT(*) FROM `" + dataset() + "`.`" + tableName + "`"));
    assertEquals(1, tableResult.getTotalRows());
    FieldValueList fieldValueList = tableResult.iterateAll().iterator().next();
    return fieldValueList.get(0).getLongValue();
  }

  private Object convertField(Field fieldSchema, FieldValue field) {
    if (field.isNull()) {
      return null;
    }
    switch (field.getAttribute()) {
      case PRIMITIVE:
        if (fieldSchema.getType().equals(BOOLEAN)) {
          return field.getBooleanValue();
        } else if (fieldSchema.getType().equals(BYTES)) {
          // Do this in order for assertEquals() to work when this is an element of two compared
          // lists
          return boxByteArray(field.getBytesValue());
        } else if (fieldSchema.getType().equals(DATE)) {
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
          return LocalDate.parse(field.getStringValue(), dateFormatter)
              .atStartOfDay(ZoneOffset.UTC)
              .toInstant()
              .toEpochMilli();
        } else if (fieldSchema.getType().equals(TIME)) {
          return field.getStringValue();
        } else if (fieldSchema.getType().equals(DATETIME)) {
          // return micro seconds.
          Instant instant =
              LocalDateTime.parse(field.getStringValue(), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                  .atOffset(ZoneOffset.UTC)
                  .toInstant();
          return instant.getEpochSecond() * 1_000_000 + instant.getNano() / 1_000;
        } else if (fieldSchema.getType().equals(FLOAT)) {
          return field.getDoubleValue();
        } else if (fieldSchema.getType().equals(INTEGER)) {
          return field.getLongValue();
        } else if (fieldSchema.getType().equals(STRING)) {
          return field.getStringValue();
        } else if (fieldSchema.getType().equals(TIMESTAMP)) {
          return field.getTimestampValue();
        } else if (fieldSchema.getType().equals(BIGNUMERIC)
            || fieldSchema.getType().equals(NUMERIC)) {
          return field.getNumericValue();
        } else {
          throw new RuntimeException(
              "Cannot convert primitive field type " + fieldSchema.getType());
        }
      case REPEATED:
        List<Object> result = new ArrayList<>();
        for (FieldValue arrayField : field.getRepeatedValue()) {
          result.add(convertField(fieldSchema, arrayField));
        }
        return result;
      case RECORD:
        List<Field> recordSchemas = fieldSchema.getSubFields();
        List<FieldValue> recordFields = field.getRecordValue();
        return convertRow(recordSchemas, recordFields);
      default:
        throw new RuntimeException("Unknown field attribute: " + field.getAttribute());
    }
  }

  private List<Object> convertRow(final List<Field> rowSchema, final List<FieldValue> row) {
    List<Object> result = new ArrayList<>();
    assert (rowSchema.size() == row.size());

    for (int i = 0; i < rowSchema.size(); i++) {
      result.add(convertField(rowSchema.get(i), row.get(i)));
    }

    return result;
  }

  /**
   * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the
   * given name to start the specified number of tasks.
   *
   * @param name the name of the connector
   * @param numTasks the minimum number of tasks that are expected
   * @throws InterruptedException if this was interrupted
   */
  protected void waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    waitForCondition(
        () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
        CONNECTOR_STARTUP_DURATION_MS,
        "Connector tasks did not start in time: " + connectorStatus);
  }

  /**
   * Confirm that a connector with an exact number of tasks is running.
   *
   * @param connectorName the connector
   * @param numTasks the minimum number of tasks
   * @return an Optional {@code true} if the connector and tasks are in RUNNING state; {@code false}
   *     if they are not and an empty Optional if there was an Exception thrown.
   */
  protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      List<String> msgs = new ArrayList<>();
      if (info == null) {
        msgs.add("Could not retrieve connector status.");
      } else {
        if (info.tasks().size() < numTasks) {
          msgs.add(
              String.format("Too few tasks expected %s got %s.", info.tasks().size(), numTasks));
        }
        if (!info.connector().state().equals(AbstractStatus.State.RUNNING.toString())) {
          msgs.add("Connector state is " + info.connector().state());
        }
        info.tasks().stream()
            .filter(s -> !s.state().equals(AbstractStatus.State.RUNNING.toString()))
            .forEach(
                ts ->
                    msgs.add(
                        String.format("Task %s is not running: %s.", ts.workerId(), ts.trace())));
      }
      connectorStatus = msgs.isEmpty() ? null : String.join(System.lineSeparator(), msgs);
      return Optional.of(msgs.isEmpty());
    } catch (Exception e) {
      logger.warn("Could not check connector state info.", e);
      connectorStatus = null;
      return Optional.empty();
    }
  }

  @Deprecated
  protected String suffixedTableOrTopic(String tableOrTopic) {
    return tableOrTopic + tableSuffix();
  }

  @Deprecated
  protected String sanitizedTable(String table) {
    return FieldNameSanitizer.sanitizeName(table);
  }

  @Deprecated
  protected String suffixedAndSanitizedTable(String table) {
    return sanitizedTable(suffixedTableOrTopic(table));
  }

  private String readEnvVar(String var) {
    String result = System.getenv(var);
    if (StringUtils.isEmpty(result)) {
      throw new IllegalStateException(
          String.format(
              "Environment variable '%s' must be supplied to run integration tests", var));
    }
    return result.trim();
  }

  private String readEnvVar(String var, String defaultVal) {
    return System.getenv().getOrDefault(var, defaultVal).trim();
  }

  protected String keyFile() {
    if (GcpClientBuilder.KeySource.APPLICATION_DEFAULT.name().equalsIgnoreCase(keySource())) {
      // Key file is optional for most tests when using application default credentials
      return readEnvVar(KEYFILE_ENV_VAR, "");
    } else {
      // Key file is required
      return readEnvVar(KEYFILE_ENV_VAR);
    }
  }

  protected String project() {
    return readEnvVar(PROJECT_ENV_VAR);
  }

  protected String dataset() {
    return readEnvVar(DATASET_ENV_VAR);
  }

  protected String keySource() {
    return readEnvVar(KEY_SOURCE_ENV_VAR, BigQuerySinkConfig.KEY_SOURCE_DEFAULT);
  }

  protected String gcsBucket() {
    return readEnvVar(GCS_BUCKET_ENV_VAR).trim();
  }

  protected String gcsFolder() {
    return readEnvVar(GCS_FOLDER_ENV_VAR, BigQuerySinkConfig.GCS_FOLDER_NAME_DEFAULT);
  }

  protected String tableSuffix() {
    return readEnvVar(TEST_NAMESPACE_ENV_VAR, defaultSuffix);
  }
}
