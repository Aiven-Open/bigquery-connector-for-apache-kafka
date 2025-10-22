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

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.wepay.kafka.connect.bigquery.write.row.GcsToBqWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Runnable that runs a GCS to BQ Load task.
 *
 * <p>This task goes through the given GCS bucket, and takes as many blobs as a single load job per
 * table can handle (as defined here: https://cloud.google.com/bigquery/quotas#load_jobs) and runs
 * those load jobs. Blobs are deleted (only) once a load job involving that blob succeeds.
 */
public class GcsToBqLoadRunnable implements Runnable {
  public static final Pattern METADATA_TABLE_PATTERN =
      Pattern.compile("((?P<project>[^:]+):)?(?P<dataset>[^.]+)\\.(?P<table>.+)");
  private static final Logger logger = LoggerFactory.getLogger(GcsToBqLoadRunnable.class);
  // max number of files we can load in a single load job
  private static int FILE_LOAD_LIMIT = 10000;
  // max total size (in bytes) of the files we can load in a single load job.
  private static long MAX_LOAD_SIZE_B = 15 * 1000000000000L; // 15TB
  private static String SOURCE_URI_FORMAT = "gs://%s/%s";
  private final BigQuery bigQuery;

  // these numbers are intended to try to make this task not exceed Google Cloud Quotas.
  // see: https://cloud.google.com/bigquery/quotas#load_jobs
  private final Bucket bucket;
  private final Map<Job, List<BlobId>> activeJobs;
  /**
   * The set of blob Ids that the system is currently processing or are queued to process.
   */
  private final Set<BlobId> claimedBlobIds;
  /**
   * The set of blob Ids that the system can delete.
   */
  private final Set<BlobId> deletableBlobIds;

  /**
   * Create a {@link GcsToBqLoadRunnable} with the given bigquery, bucket, and ms wait interval.
   *
   * @param bigQuery the {@link BigQuery} instance.
   * @param bucket   the GCS bucket to read from.
   */
  public GcsToBqLoadRunnable(BigQuery bigQuery, Bucket bucket) {
    this.bigQuery = bigQuery;
    this.bucket = bucket;
    this.activeJobs = new HashMap<>();
    this.claimedBlobIds = new HashSet<>();
    this.deletableBlobIds = new HashSet<>();
  }

  /**
   * Create a {@link GcsToBqLoadRunnable} with the given bigquery, bucket, and ms wait interval.
   *
   * @param bigQuery the {@link BigQuery} instance.
   * @param bucket   the GCS bucket to read from.
   * @param activeJobs the map of job to the list of blobs it contains.
   * @param claimedBlobIds the list of Blob Ids being processed.
   * @param deletableBlobIds the list of Blob Ids that can be deleted.
   */
  @VisibleForTesting
  GcsToBqLoadRunnable(BigQuery bigQuery, Bucket bucket, Map<Job, List<BlobId>> activeJobs, Set<BlobId> claimedBlobIds, Set<BlobId> deletableBlobIds) {
    this.bigQuery = bigQuery;
    this.bucket = bucket;
    this.activeJobs = activeJobs;
    this.claimedBlobIds = claimedBlobIds;
    this.deletableBlobIds = deletableBlobIds;
  }

  /**
   * Given a blob, return the {@link TableId} this blob should be inserted into.
   *
   * @param blob the blob
   * @return the TableId this data should be loaded into, or null if we could not tell what
   * table it should be loaded into.
   */
  public static TableId getTableFromBlob(Blob blob) {
    if (blob.getMetadata() == null
        || blob.getMetadata().get(GcsToBqWriter.GCS_METADATA_TABLE_KEY) == null) {
      logger.error("Found blob {}/{} with no metadata.", blob.getBucket(), blob.getName());
      return null;
    }

    String serializedTableId = blob.getMetadata().get(GcsToBqWriter.GCS_METADATA_TABLE_KEY);
    Matcher matcher = METADATA_TABLE_PATTERN.matcher(serializedTableId);

    if (!matcher.find()) {
      logger.error("Found blob `{}/{}` with un-parsable table metadata.",
          blob.getBucket(), blob.getName());
      return null;
    }

    String project = matcher.group("project");
    String dataset = matcher.group("dataset");
    String table = matcher.group("table");
    logger.debug("Table data: project: {}; dataset: {}; table: {}", project, dataset, table);

    if (project == null) {
      return TableId.of(dataset, table);
    } else {
      return TableId.of(project, dataset, table);
    }
  }

  /**
   * Return a map of {@link TableId}s to a list of {@link Blob}s intended to be batch-loaded into
   * that table.
   *
   * <p>Each blob list will not exceed the {@link #FILE_LOAD_LIMIT} in number of blobs or
   * {@link #MAX_LOAD_SIZE_B} in total byte size. Blobs that are already claimed by an in-progress
   * load job will also not be included.
   *
   * @return map from {@link TableId}s to {@link Blob}s.
   */
  private Map<TableId, List<Blob>> getBlobsUpToLimit() {
    Map<TableId, List<Blob>> tableToUris = new HashMap<>();
    Map<TableId, Long> tableToCurrentLoadSize = new HashMap<>();

    logger.trace("Starting GCS bucket list");
    Page<Blob> list = bucket.list();
    logger.trace("Finished GCS bucket list");

    for (Blob blob : list.iterateAll()) {
      BlobId blobId = blob.getBlobId();
      TableId table = getTableFromBlob(blob);
      logger.debug("Checking blob bucket={}, name={}, table={} ", blob.getBucket(), blob.getName(), table);

      if (table == null || claimedBlobIds.contains(blobId) || deletableBlobIds.contains(blobId)) {
        // don't do anything if:
        // 1. we don't know what table this should be uploaded to or
        // 2. this blob is already claimed by a currently-running job or
        // 3. this blob is up for deletion.
        continue;
      }

      if (!tableToUris.containsKey(table)) {
        // initialize maps, if we haven't seen this table before.
        tableToUris.put(table, new ArrayList<>());
        tableToCurrentLoadSize.put(table, 0L);
      }

      long newSize = tableToCurrentLoadSize.get(table) + blob.getSize();
      // if this file does not cause us to exceed our per-request quota limits...
      if (newSize < MAX_LOAD_SIZE_B && tableToUris.get(table).size() < FILE_LOAD_LIMIT) {
        // ...add the file (and update the load size)
        tableToUris.get(table).add(blob);
        tableToCurrentLoadSize.put(table, newSize);
      }
    }

    logger.debug("Got blobs to upload: {}", tableToUris);
    return tableToUris;
  }

  /**
   * Trigger a BigQuery load job for each table in the input containing all the blobs associated
   * with that table.
   *
   * @param tablesToBlobs a map of {@link TableId} to the list of {@link Blob}s to be loaded into
   *                      that table.
   * @return a map from Jobs to the list of blobs being loaded in that job.
   */
  private Map<Job, List<Blob>> triggerBigQueryLoadJobs(Map<TableId, List<Blob>> tablesToBlobs) {
    Map<Job, List<Blob>> newJobs = new HashMap<>(tablesToBlobs.size());
    for (Map.Entry<TableId, List<Blob>> entry : tablesToBlobs.entrySet()) {
      newJobs.put(triggerBigQueryLoadJob(entry.getKey(), entry.getValue()), entry.getValue());
    }
    return newJobs;
  }

  private Job triggerBigQueryLoadJob(TableId table, List<Blob> blobs) {
    List<String> uris = blobs.stream()
        .map(b -> String.format(SOURCE_URI_FORMAT,
            bucket.getName(),
            b.getName()))
        .collect(Collectors.toList());
    // create job load configuration
    LoadJobConfiguration loadJobConfiguration =
        LoadJobConfiguration.newBuilder(table, uris)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();
    // create and return the job.
    Job job = bigQuery.create(JobInfo.of(loadJobConfiguration));
    // update active jobs and claimed blobs.
    List<BlobId> blobIds = blobs.stream().map(Blob::getBlobId).collect(Collectors.toList());
    activeJobs.put(job, blobIds);
    claimedBlobIds.addAll(blobIds);
    logger.info("Triggered load job for table {} with {} blobs.", table, blobs.size());
    return job;
  }

  /**
   * Check all active jobs. Remove those that have completed successfully and log a message for
   * any jobs that failed. We only log a message for failed jobs because those blobs will be
   * retried during the next run.
   */
  @VisibleForTesting
  void checkJobs() {
    if (activeJobs.isEmpty()) {
      // quick exit if nothing needs to be done.
      logger.debug("No active jobs to check. Skipping check jobs.");
      return;
    }
    logger.debug("Checking {} active jobs", activeJobs.size());

    Iterator<Map.Entry<Job, List<BlobId>>> jobIterator = activeJobs.entrySet().iterator();
    int successCount = 0;
    int failureCount = 0;

    while (jobIterator.hasNext()) {
      Map.Entry<Job, List<BlobId>> jobEntry = jobIterator.next();
      Job job = jobEntry.getKey();
      logger.debug("Checking next job: {}", job.getJobId());

      job = bigQuery.getJob(job.getJobId());
      try {
        if (job.getStatus().getState() == JobStatus.State.DONE) {
          logger.trace("Job is marked done: id={}, status={}", job.getJobId(), (job.getStatus());
          if (job.getStatus().getError() == null) {
            processSuccessfulJob(job, jobEntry.getValue());
            successCount++;
          } else {
            processFailedJob(job, jobEntry.getValue());
            failureCount++;
          }
          jobIterator.remove();
          logger.trace("Job is removed from iterator: {}", job.getJobId());
        }
      } catch (BigQueryException ex) {
        // log a message.
        logger.warn("GCS to BQ load job failed", ex);
        processFailedJob(job, jobEntry.getValue());
        failureCount++;
        jobIterator.remove();
        logger.trace("Job is removed from iterator: {}", job.getJobId());
      } finally {
        logger.info("GCS To BQ job tally: {} successful jobs, {} failed jobs.",
            successCount, failureCount);
      }
    }
  }

  private void processSuccessfulJob(final Job job, final List<BlobId> blobIdsToDelete) {
    blobIdsToDelete.forEach(claimedBlobIds::remove);
    logger.trace("Completed blobs have been removed from claimed set: {}", blobIdsToDelete);
    deletableBlobIds.addAll(blobIdsToDelete);
    logger.trace("Completed blobs marked as deletable: {}", blobIdsToDelete);
  }

  private void processFailedJob(final Job job, final List<BlobId> blobsNotCompleted) {
    logger.warn("Job {} failed with {}", job.getJobId(), (job.getStatus().getError());
    if ((job.getStatus().getExecutionErrors().isEmpty()) {
      logger.warn("No additional errors associated with job {}", job.getJobId());
    } else {
      logger.warn("Additional errors associated with job {}: {}", job.getJobId(), (job.getStatus().getExecutionErrors());
    }
    logger.warn("Blobs in job {}: {}", job.getJobId(), blobsNotCompleted);
    // unclaim blobs
    blobsNotCompleted.forEach(claimedBlobIds::remove);
    logger.trace("Failed blobs reset as processable");
  }

  /**
   * Delete deletable blobs.
   */
  private void deleteBlobs() {
    List<BlobId> blobIdsToDelete = new ArrayList<>();
    blobIdsToDelete.addAll(deletableBlobIds);
    int numberOfBlobs = blobIdsToDelete.size();
    int failedDeletes = 0;
    int successfulDeletes = 0;

    if (numberOfBlobs == 0) {
      logger.info("No blobs to delete");
      return;
    }

    logger.info("Attempting to delete {} blobs", numberOfBlobs);

    try {
      // Issue a batch delete api call
      List<Boolean> resultList = bucket.getStorage().delete(blobIdsToDelete);

      // Filter the blobs we couldn't delete from the list of deletable blobs
      for (int i = 0; i < numberOfBlobs; i++) {
        if (!resultList.get(i)) {
          // This blob was not successful, remove it from the list.
          // Adjust the target index by the number of failed deletes we've
          // already seen since we're mutating the list as we go.
          int targetIndex = i - failedDeletes;
          blobIdsToDelete.remove(targetIndex);
          failedDeletes++;
        }
      }

      // Calculate number of successful deletes, remove the successful deletes from
      // the deletableBlobIds.
      successfulDeletes = numberOfBlobs - failedDeletes;
      blobIdsToDelete.forEach(deletableBlobIds::remove);

      logger.info("Successfully deleted {} blobs; failed to delete {} blobs",
          successfulDeletes,
          failedDeletes);
    } catch (StorageException ex) {
      logger.warn("Storage exception while attempting to delete blobs", ex);
    }
  }

  @Override
  public void run() {
    logger.trace("Starting BQ load run");
    try {
      logger.trace("Checking for finished job statuses. Moving uploaded blobs from claimed to deletable.");
      checkJobs();
      logger.trace("Deleting deletable blobs");
      deleteBlobs();
      logger.trace("Finding new blobs to load into BQ");
      Map<TableId, List<Blob>> tablesToSourceUris = getBlobsUpToLimit();
      logger.trace("Loading {} new blobs into BQ", tablesToSourceUris.size());
      triggerBigQueryLoadJobs(tablesToSourceUris);
      logger.trace("Finished BQ load run");
    } catch (Exception e) {
      logger.error("Uncaught error in BQ loader", e);
    }
  }
}
