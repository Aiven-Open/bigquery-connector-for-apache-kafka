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

import static com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.wepay.kafka.connect.bigquery.exception.BigQueryErrorResponses;
import com.wepay.kafka.connect.bigquery.integration.utils.BigQueryTestUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.wepay.kafka.connect.bigquery.utils.TableNameUtils;
import org.apache.kafka.test.TestUtils;
import org.assertj.core.api.Condition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryErrorResponsesIT extends BaseConnectorIT {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryErrorResponsesIT.class);

  private BigQuery bigQuery;

  @BeforeEach
  public void setup() {
    bigQuery = newBigQuery();
  }

  @AfterEach
  void teardown() {
    delete(bigQuery, tableName());
  }


  @Test
  public void testWriteToNonExistentTable() {
    TableId table = TableNameUtils.tableId(tableName());

    assertThatThrownBy(() -> bigQuery.insertAll(
            InsertAllRequest.of(
                    table, RowToInsert.of(Collections.singletonMap("f1", "v1")))))
            .isInstanceOf(BigQueryException.class)
            .is(new Condition<>(e -> BigQueryErrorResponses.isNonExistentTableError((BigQueryException)e), "Nonexistent table write error"));
  }

  @Test
  public void testWriteToRecreatedTable() throws Exception {
    TableName tableName = tableName();

    Schema schema = Schema.of(Field.of("f1", LegacySQLTypeName.STRING));

    // Create the table...
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    // Delete it...
    delete(bigQuery, tableName);

    // Make sure that it's deleted
    TableId tableId = TableNameUtils.tableId(tableName);
    Awaitility.await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> assertThat(bigQuery.getTable(tableId)).isNull());

    final ExceptionTracker exceptionTracker = new ExceptionTracker();

    TestUtils.waitForCondition(
        () -> {
          // Try to write to it...
          try {
            bigQuery.insertAll(
                InsertAllRequest.of(tableId, RowToInsert.of(Collections.singletonMap("f1", "v1"))));
            return false;
          } catch (BigQueryException e) {
            if (BigQueryErrorResponses.isNonExistentTableError(e)) {
              logger.debug("Deleted table write error: {}", e.getMessage());
              return true;
            }
            logger.info("Unexpected error: {}", exceptionTracker.recordException(e).getMessage());
            return false;
          }
        },
        ONE_MINUTE,
        exceptionTracker.report("Never failed to write to just-deleted table."));

    // Recreate it...
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    exceptionTracker.reset();

    // this one takes time so only check every second.
    //Awaitility.waitAtMost(Duration.ofMinutes(1)).untilAsserted();
    TestUtils.waitForCondition(
        () -> {
          // Try to write to it...
          try {
            bigQuery.insertAll(
                InsertAllRequest.of(tableId, RowToInsert.of(Collections.singletonMap("f1", "v1"))));
            return true;
          } catch (BigQueryException e) {
            logger.debug(
                "Recreated table write error: {}",
                exceptionTracker.recordException(e).getMessage());
            return false;
          }
        },
        ONE_MINUTE,
        ONE_SECOND,
        () -> exceptionTracker.report("Never succeeded to write to just-recreated table."));
  }

  @Test
  public void testWriteToTableWithoutSchema() {

    TableName tableName = tableName();
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, Schema.of());
    TableId tableId = TableNameUtils.tableId(tableName);

    assertThatThrownBy(() -> bigQuery.insertAll(
            InsertAllRequest.of(
                    tableId, RowToInsert.of(Collections.singletonMap("f1", "v1")))))
            .isInstanceOf(BigQueryException.class)
            .is(new Condition<>(e -> BigQueryErrorResponses.isTableMissingSchemaError((BigQueryException)e), "Table missing schema write error"));
  }

  @Test
  public void testWriteWithMissingRequiredFields() {
    TableName tableName = tableName();
    TableId tableId = TableNameUtils.tableId(tableName);
    Schema schema =
        Schema.of(
            Field.newBuilder("f1", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("f2", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("f3", StandardSQLTypeName.BOOL).setMode(Field.Mode.NULLABLE).build());

    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.of(tableId, RowToInsert.of(Collections.singletonMap("f1", "v1"))));
    BigQueryError error = assertResponseHasSingleError(response);
    assertThat(BigQueryErrorResponses.isMissingRequiredFieldError(error)).isTrue();
  }

  @Test
  public void testWriteWithUnrecognizedFields() {
    TableName tableName = tableName();
    TableId tableId = TableNameUtils.tableId(tableName);
    Schema schema =
        Schema.of(
            Field.newBuilder("f1", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build());
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    Map<String, Object> row = new HashMap<>();
    row.put("f1", "v1");
    row.put("f2", 12L);
    InsertAllResponse response =  bigQuery.insertAll(InsertAllRequest.of(tableId, RowToInsert.of(row)));
    BigQueryError error = assertResponseHasSingleError(response);
    assertThat(BigQueryErrorResponses.isUnrecognizedFieldError(error)).isTrue();
  }

  @Test
  public void testStoppedRowsDuringInvalidWrite() {
    TableName tableName = tableName();
    TableId tableId = TableNameUtils.tableId(tableName);
    Schema schema =
        Schema.of(
            Field.newBuilder("f1", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build());
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    Map<String, Object> row1 = new HashMap<>();
    row1.put("f1", "v1");
    row1.put("f2", 12L);
    Map<String, Object> row2 = Collections.singletonMap("f1", "v2");
    InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.of(tableId, RowToInsert.of(row1), RowToInsert.of(row2)));
    assertThat(response.getInsertErrors()).hasSize(2);

    // As long as we have some kind of error on the first row it's fine; we want to be more precise
    // in our assertions about the second row
    assertListHasSingleElement(response.getErrorsFor(0));
    BigQueryError secondRowError = assertListHasSingleElement(response.getErrorsFor(1));
    assertTrue(BigQueryErrorResponses.isStoppedError(secondRowError));
  }

  @Test
  public void testRequestPayloadTooLarge() {
    TableName tableName = tableName();
    TableId tableId = TableNameUtils.tableId(tableName);
    Schema schema =
        Schema.of(
            Field.newBuilder("f1", StandardSQLTypeName.STRING)
                .setMode(Field.Mode.REQUIRED)
                .build());
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    char[] chars = new char[10 * 1024 * 1024];
    Arrays.fill(chars, '*');
    String columnValue = new String(chars);

    assertThatThrownBy(() -> bigQuery.insertAll(InsertAllRequest.of(
                    tableId, RowToInsert.of(Collections.singletonMap("f1", columnValue)))))
            .isInstanceOf(BigQueryException.class)
            .is(new Condition<>(e -> BigQueryErrorResponses.isRequestTooLargeError((BigQueryException)e), "Large request payload write error"));
  }

  @Test
  public void testTooManyRows() {
    TableName tableName = tableName();
    TableId tableId = TableNameUtils.tableId(tableName);
    Schema schema =
        Schema.of(
            Field.newBuilder("f1", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build());
    BigQueryTestUtils.createStandardTable(bigQuery, tableName, schema);

    List<RowToInsert> rows =
        LongStream.range(0, 100_000)
            .mapToObj(i -> Collections.singletonMap("f1", i))
            .map(RowToInsert::of)
            .collect(Collectors.toList());


    assertThatThrownBy(() -> bigQuery.insertAll(InsertAllRequest.of(tableId, rows)))
            .isInstanceOf(BigQueryException.class)
            .is(new Condition<>(e -> BigQueryErrorResponses.isTooManyRowsError((BigQueryException)e), "To mny rows write error"));
  }

  private BigQueryError assertResponseHasSingleError(InsertAllResponse response) {
    assertEquals(1, response.getInsertErrors().size());
    Iterator<List<BigQueryError>> errorsIterator = response.getInsertErrors().values().iterator();
    assertTrue(errorsIterator.hasNext());
    return assertListHasSingleElement(errorsIterator.next());
  }

  private <T> T assertListHasSingleElement(List<T> list) {
    assertEquals(1, list.size());
    return list.get(0);
  }

  /** Tracks the latest BigQueryException. */
  private static final class ExceptionTracker {
    private BigQueryException lastError = null;

    /**
     * Record the occurrence of the exception.
     *
     * @param exception the exception that was thrown.
     * @return the exception.
     */
    public BigQueryException recordException(BigQueryException exception) {
      lastError = exception;
      return exception;
    }

    /**
     * Produces a report for the exception, if any. Adds the exception stack trace to the base
     * message if an exception was thrown. Otherwise, returns the base message. May be used in lamda
     * expressions to track exceptions and output detailed reports.
     *
     * @param baseMsg the basic message.
     * @return the report message.
     */
    public String report(final String baseMsg) {
      try (StringWriter sr = new StringWriter();
          PrintWriter writer = new PrintWriter(sr)) {
        writer.append(baseMsg);
        if (lastError != null) {
          writer.append(" Latest exception: ");
          lastError.printStackTrace(writer);
        }
        writer.flush();
        return sr.toString();
      } catch (IOException e) {
        return baseMsg;
      }
    }

    /**
     * Returns an Optional containing the last exception, if one was thrown, otherwise, an empty
     * Optional.
     *
     * @return an Optional containing the last exception thrown.
     */
    public Optional<BigQueryException> getException() {
      return Optional.ofNullable(lastError);
    }

    /** Resets the last error so that this tracker can be reused. */
    public void reset() {
      lastError = null;
    }
  }
}
