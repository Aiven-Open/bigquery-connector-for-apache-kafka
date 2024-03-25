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

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import org.json.JSONArray;

public interface StreamWriter {

  /**
   * Write the provided rows
   *
   * @param rows the rows to write; may not be null
   * @return the response from BigQuery for the write attempt
   */
  ApiFuture<AppendRowsResponse> appendRows(
      JSONArray rows
  ) throws Descriptors.DescriptorValidationException, IOException;

  /**
   * Invoked if the underlying stream appears to be closed. Implementing classes
   * should respond by re-initialize the underlying stream.
   */
  void refresh();

  /**
   * Invoked when all rows have either been written to BigQuery or intentionally
   * discarded (e.g., reported to an {@link com.wepay.kafka.connect.bigquery.ErrantRecordHandler}).
   */
  void onSuccess();

  String streamName();

}
