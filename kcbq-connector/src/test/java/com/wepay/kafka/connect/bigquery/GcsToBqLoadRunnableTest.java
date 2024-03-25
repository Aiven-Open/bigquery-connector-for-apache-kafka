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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GcsToBqLoadRunnableTest {

  @Test
  public void testGetTableFromBlobWithProject() {
    final TableId expectedTableId = TableId.of("project", "dataset", "table");

    Map<String, String> metadata =
        Collections.singletonMap("sinkTable", serializeTableId(expectedTableId));
    Blob mockBlob = createMockBlobWithTableMetadata(metadata);

    TableId actualTableId = GcsToBqLoadRunnable.getTableFromBlob(mockBlob);
    assertEquals(expectedTableId, actualTableId);
  }

  @Test
  public void testGetTableFromBlobWithoutProject() {
    final TableId expectedTableId = TableId.of("dataset", "table");

    Map<String, String> metadata =
        Collections.singletonMap("sinkTable", serializeTableId(expectedTableId));
    Blob mockBlob = createMockBlobWithTableMetadata(metadata);

    TableId actualTableId = GcsToBqLoadRunnable.getTableFromBlob(mockBlob);
    assertEquals(expectedTableId, actualTableId);
  }

  @Test
  public void testGetTableFromBlobWithoutMetadata() {
    Blob mockBlob = mock(Blob.class);
    Mockito.when(mockBlob.getMetadata()).thenReturn(null);

    TableId tableId = GcsToBqLoadRunnable.getTableFromBlob(mockBlob);
    assertNull(tableId);
  }

  @Test
  public void testGetTableFromBlobWithBadMetadata() {
    Map<String, String> metadata = Collections.singletonMap("sinkTable", "bar/baz");
    Blob mockBlob = createMockBlobWithTableMetadata(metadata);

    TableId tableId = GcsToBqLoadRunnable.getTableFromBlob(mockBlob);
    assertNull(tableId);
  }

  private String serializeTableId(TableId tableId) {
    final String project = tableId.getProject();
    final String dataset = tableId.getDataset();
    final String table = tableId.getTable();
    StringBuilder sb = new StringBuilder();
    if (project != null) {
      sb.append(project).append(":");
    }
    return sb.append(dataset).append(".").append(table).toString();
  }

  private Blob createMockBlobWithTableMetadata(Map<String, String> metadata) {
    Blob mockBlob = mock(Blob.class);
    Mockito.when(mockBlob.getMetadata()).thenReturn(metadata);
    return mockBlob;
  }


}
