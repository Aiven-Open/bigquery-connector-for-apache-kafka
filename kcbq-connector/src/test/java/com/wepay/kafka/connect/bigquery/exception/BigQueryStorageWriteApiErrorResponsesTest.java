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

package com.wepay.kafka.connect.bigquery.exception;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class BigQueryStorageWriteApiErrorResponsesTest {

  @Test
  public void testTableMissingDueToPermissionDenied() {
    String message = "PERMISSION_DENIED on resource table abc (or it may not exist)";
    boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
    assertTrue(result);
  }

  @Test
  public void testTableMissingDueToNotFound() {
    String message = "Not found: table abc";
    boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
    assertTrue(result);
  }

  @Test
  public void testTableMissingDueToDeleted() {
    String message = "Not found or Table is deleted";
    boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
    assertTrue(result);
  }

  @Test
  public void testTableNotMissing() {
    String message = "INTERNAL: internal error occurred";
    boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
    assertFalse(result);
  }

  @Test
  public void testRetriableInternal() {
    String message = "INTERNAL: internal error occurred";
    boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
    assertTrue(result);
  }

  @Test
  public void testRetriableAborted() {
    String message = "ABORTED: operation is aborted";
    boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
    assertTrue(result);
  }

  @Test
  public void testRetriableCancelled() {
    String message = "CANCELLED: stream cancelled on user action";
    boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
    assertTrue(result);
  }

  @Test
  public void testMalformedRequest() {
    Map<Integer, String> errors = new HashMap<>();
    errors.put(0, "JSONObject has fields unknown to BigQuery: root.f1.");
    String message = "INVALID_ARGUMENT:  JSONObject has fields unknown to BigQuery: root.f1.";
    Exceptions.AppendSerializtionError error = new Exceptions.AppendSerializtionError(
        3,
        message,
        "DEFAULT",
        errors);
    boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(error.getMessage());

    assertTrue(result);
  }

  @Test
  public void testNonInvalidArgument() {
    Map<Integer, String> errors = new HashMap<>();
    String message = "Deadline Exceeded";
    Exceptions.AppendSerializtionError error = new Exceptions.AppendSerializtionError(
        13,
        message,
        "DEFAULT",
        errors);
    boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(error.getMessage());

    assertFalse(result);
  }

  @Test
  public void testNonMalformedException() {
    String message = "Deadline Exceeded";
    Exception e = new Exception(message);
    boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e.getMessage());

    assertFalse(result);
  }

  @Test
  public void testHasInvalidSchema() {
    Collection<String> errors = new ArrayList<>();
    errors.add("The source object has malformed field with length 5, specified length 3");
    errors.add("The source object has fields unknown to BigQuery root.f1");
    boolean result = BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(errors);
    assertTrue(result);
  }

  @Test
  public void testInvalidSchemaHasMoreFieldsThanBigQuerySchema() {
    Collection<String> errors = new ArrayList<>();
    errors.add("INVALID_ARGUMENT: Input schema has more fields than BigQuery schema, extra fields:");
    boolean result = BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(errors);
    assertTrue(result);
  }

  @Test
  public void testHasInvalidStorageSchema() {
    Collection<String> errors = new ArrayList<>();
    errors.add("Failed to write records due to SCHEMA_MISMATCH_EXTRA_FIELDS");
    boolean result = BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(errors);
    assertTrue(result);
  }

  @Test
  public void testHasNoInvalidSchema() {
    Collection<String> errors = new ArrayList<>();
    errors.add("JSONObject has malformed field with length 5, specified length 3");
    errors.add("JSONObject has fields specified twice");
    boolean result = BigQueryStorageWriteApiErrorResponses.hasInvalidSchema(errors);
    assertFalse(result);
  }

  @Test
  public void testStreamClosed() {
    String message = "ExecutionException$StreamWriterClosedException due to FAILED PRE_CONDITION";
    boolean result = BigQueryStorageWriteApiErrorResponses.isStreamClosed(message);
    assertTrue(result);
  }

  @Test
  public void testIsNonRetriableStorageError() {
    Exception exception = new Exception(
        io.grpc.Status.fromThrowable(new Throwable())
            .withDescription("STREAM_FINALIZED")
            .asRuntimeException()
    );

    boolean result = BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(exception);
    assertTrue(result);
  }

  @Test
  public void testIsNonStorageError() {
    Exception exception = new Exception("I am not a storage error");
    boolean result = BigQueryStorageWriteApiErrorResponses.isNonRetriableStorageError(exception);
    assertTrue(result);
  }
}

