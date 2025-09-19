# Write API Best Practices
Author: [Siddharth Agrawal](mailto:siddag@google.com) | Date: Sep 18, 2025

This document builds upon a set of [best practices](https://cloud.google.com/bigquery/docs/write-api-best-practices)
to follow when using the [BigQuery Write API](https://cloud.google.com/bigquery/docs/write-api).
These practices are being incorporated into the Aiven BigQuery Sink Connector.

**_TABLE OF CONTENTS_**

[Enable Retries](#enable-java-client-side-retries-for-storage-write-api)

[Enable Multiplexing](#enable-multiplexing-for-storage-write-api)

## Enable Java Client-Side Retries for Storage Write API

Author: [Mariia Podgaietska](mailto:mpodgaietska@google.com) | Date: Aug 5, 2025

### Objective

Enable the built-in retry mechanism provided by the Storage Write API Java
client library to handle request-level errors within the BigQuery Sink Connector.

### Motivation

The connector currently implements its own retry mechanism to handle failures
encountered during append rows calls. The BigQuery Storage Write API Java client
library, however, already includes built-in and configurable retries
([see documentation](https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/3.5.0/com.google.cloud.bigquery.storage.v1.JsonStreamWriter.Builder#com_google_cloud_bigquery_storage_v1_JsonStreamWriter_Builder_setRetrySettings_com_google_api_gax_retrying_RetrySettings_))
for handling request-level transient errors.

By deferring handling of request-level error retries to the Java client's
internal retry mechanism, the connector can benefit from more advanced and
tunable retry behaviour, such as exponential backoff, etc. This would also
help simplify the connector's logic and enable a more standardized way of
handling transient errors.

### Design

#### Java Client Retry Configuration

When instantiating a <span style="color:green">JsonStreamWriter</span>,
the connector will construct and apply a <span style="color:green">RetrySettings</span>
object to enable the Java client's built-in retry mechanism. Existing [connector configurations](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/blob/main/docs/sink-connector-config-options.rst#:~:text=Importance%3A%20medium-,bigQueryRetry,Importance%3A%20medium,-gcsFolderName)
(<span style="color:green">bigQueryRetry</span> and <span style="color:green">bigQueryRetryWait</span>),
which currently control connector-level retries, will be reused to set the
<span style="color:green">maxAttempts</span> and <span style="color:green">initialRetryDelay</span>
respectively. Other useful configurations could be set with default values for
now and made configurable later through additional connector configs if needed,
such as <span style="color:green">setRetryDelayMultiplier(1.1)</span>.

#### Error Handling Behaviour

The connector's existing retry logic remains important for handling error scenarios that
require intervention and then a re-attempt of the write. Examples of such scenarios are:

- Schema mismatch: connector updates table schema (if configured to do so) and reattempts
write.

- Missing table: connector creates a table (if configured to do so) and reattempts write.

- Malformed records: connector reroutes malformed records to DLQ (if configured to do so)
and reattempts write.

- Closed stream: connector recreates stream and reattempts write.

- Request too large: connector reattempts request with smaller batch size and reattempts
write.

For request-level errors (e.g, transient gRPC failures), however, we want to avoid compounded
retries resulting from both the Java client and the connector retrying requests one after
another. To achieve this, we will ensure that:

- If an error returned by append call is a known logical error (one of the listed above),
the connector will proceed with its own retry mechanism as before.

- If the error does not match any of the above cases, the task will fail immediately, as we
can assume that:

    - Append failed due to a non-retryable error, or

    - Append failed due to a request-level error, but the java client exhausted all retry
attempts

## Enable Multiplexing for Storage Write API

As explained in the [official documentation](https://cloud.google.com/bigquery/docs/write-api-best-practices#connection_pool_management),
the multiplexing or "connection management" feature is available when using
the Write API default stream. Presently this feature is turned off by default
within the Java client library. However, enabling this feature provides
benefits of:

1. Minimizing the number of connections that could be simultaneously
opened when using multiple <span style="color:green">JsonStreamWriter</span>
objects to write to one or more BigQuery tables. Instead, existing connections
will automatically be shared by traffic intended for multiple tables that
reside in the same destination region. Using fewer connections improves
efficiency of the data transfer and also avoids hitting BigQuery [connection
quota limits](https://cloud.google.com/bigquery/quotas#write-api-limits).
2. Automatically scaling up (and down) the number of connections being used to
send traffic to one or more destination tables. The scaling up (and down)
process is managed entirely within the Java client library which simplifies
client-side code. For example, let's say the connector is processing a single
topic. As the rate of traffic from this topic increases, using a single
<span style="color:green">JsonStreamWriter</span> might normally not be enough
as a single connection can get saturated at roughly 10MB/s. With multiplexing
enabled, the Java client library will automatically add new connections in the
background to handle the increasing load.

It is recommended to have multiplexing turned on whenever the default stream
is being used. For example, this practice is also [followed](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/399e1c6df5d0532c03be06968eacef506e57d914/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java#L159)
by the Spark Dataproc connector.