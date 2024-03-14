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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.storage.v1.RowError;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class BigQueryStorageWriteApiConnectExceptionTest {

  @Test
  public void testFormatRowErrorBigQueryStorageWriteApi() {
    String expectedMessage = "Insertion failed at table abc for following rows: \n " +
        "[row index 0] (Failure reason : f1 is not valid) ";
    List<RowError> errors = new ArrayList<>();
    errors.add(RowError.newBuilder().setIndex(0).setMessage("f1 is not valid").build());
    BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("abc", errors);
    assertEquals(expectedMessage, exception.getMessage());
  }

  @Test
  public void testFormatAppendSerializationErrorBigQueryStorageWriteApi() {
    String expectedMessage = "Insertion failed at table abc for following rows: \n " +
        "[row index 0] (Failure reason : f1 is not valid) \n [row index 1] (Failure reason : f2 is not valid) ";
    Map<Integer, String> errors = new HashMap<>();
    errors.put(0, "f1 is not valid");
    errors.put(1, "f2 is not valid");
    BigQueryStorageWriteApiConnectException exception = new BigQueryStorageWriteApiConnectException("abc", errors);
    assertEquals(expectedMessage, exception.getMessage());
  }
}
