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
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
    when(mockBlob.getMetadata()).thenReturn(null);

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
    when(mockBlob.getMetadata()).thenReturn(metadata);
    return mockBlob;
  }

  private static Job createJob(String jobId) {
    Job job = mock(Job.class);
    when(job.getJobId()).thenReturn(JobId.of(jobId));
    return job;

  }

  @ParameterizedTest
  @MethodSource("checkJobsData")
  void testCheckJobsFailure(String name, Job job, List<BlobId> blobIds, int activeCount, int claimedCount, int deletableCount) {
    BigQuery bigQuery = mock(BigQuery.class);
    Bucket bucket = mock(Bucket.class);
    final Map<Job, List<BlobId>> activeJobs = new HashMap<>();
    final Set<BlobId> deletableBlobIds = new HashSet<>();
    final Set<BlobId> claimedBlobIds = new HashSet<>(blobIds);
    when(bigQuery.getJob(job.getJobId())).thenReturn(job);

    activeJobs.put(job, blobIds);

    GcsToBqLoadRunnable runnable = new GcsToBqLoadRunnable(bigQuery, bucket, activeJobs, claimedBlobIds, deletableBlobIds);
    runnable.checkJobs();
    assertEquals(activeCount, activeJobs.size(), "Wrong active count" );
    assertEquals(claimedCount, claimedBlobIds.size(), "Wrong claimed count");
    assertEquals(deletableCount, deletableBlobIds.size(), "Wrong deletable count");
  }

  static List<Arguments> checkJobsData() {
    List<Arguments> args = new ArrayList<>();

    Job job = createJob("errorInProcessing");
    BlobId blob = BlobId.of("bucket", "blob1");
    BigQueryError error = new BigQueryError("reason","location", "message", "debugInfo");
    JobStatus jobStatus = mock(JobStatus.class);
    when(job.getStatus()).thenReturn(jobStatus);
    when(job.getStatus().getState()).thenReturn(JobStatus.State.DONE);
    when(jobStatus.getError()).thenReturn(error);
    when(jobStatus.getExecutionErrors()).thenReturn(Collections.singletonList(new BigQueryError("executionError","location", "message", "debugInfo")));
    args.add(Arguments.of(job.getJobId().getJob(), job, Collections.singletonList(blob), 0, 0, 0));

    job = createJob("goodCompleted");
    blob = BlobId.of("bucket", "blob2");
    jobStatus = mock(JobStatus.class);
    when(job.getStatus()).thenReturn(jobStatus);
    when(job.getStatus().getState()).thenReturn(JobStatus.State.DONE);
    args.add(Arguments.of(job.getJobId().getJob(), job, Collections.singletonList(blob), 0, 0, 1));

    job = createJob("exception");
    blob = BlobId.of("bucket", "blob3");
    when(job.isDone()).thenThrow(BigQueryException.class);
    jobStatus = mock(JobStatus.class);
    when(job.getStatus()).thenReturn(jobStatus);
    when(job.getStatus().getState()).thenThrow(BigQueryException.class);
    args.add(Arguments.of(job.getJobId().getJob(), job, Collections.singletonList(blob), 0, 0, 0));

    job = createJob("stillRunning");
    blob = BlobId.of("bucket", "blob2");
    jobStatus = mock(JobStatus.class);
    when(job.getStatus()).thenReturn(jobStatus);
    when(job.getStatus().getState()).thenReturn(JobStatus.State.PENDING);
    args.add(Arguments.of(job.getJobId().getJob(), job, Collections.singletonList(blob), 1, 1, 0));
    return args;
  }

}
