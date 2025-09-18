# BigQuery Sink Connector: Support for Ignoring Unknown Fields

Author: [Mariia Podgaietska](mailto:mpodgaietska@google.com) | Date: Aug 4, 2025

Objective
---------

Allow BigQuery Sink Connector to ingest Kafka records that contain additional fields not present in the target BigQuery table schema.

Motivation
----------

The connector currently enforces strict schema matching, rejecting any records that include fields not defined in the target BigQuery table. While this guarantees schema consistency, it can be overly restrictive for use cases where users are aware of and willing to discard extra fields, preferring to ingest the valid portion of the record instead of failing the message entirely or routing it to the DLQ. For example, wanting to process messages from a topic where records include optional enrichment metadata, without being willing to modify the BigQuery table schema to accommodate those additional fields.

Allowing the connector to ignore unknown fields in kafka records would provide greater flexibility and control over ingestion behaviour, enabling broader support for use cases where exact schema match between Kafka message and BigQuery table might not be required.

Design
------

A new opt-in connector configuration will be introduced: <span style="color:green">ignoreUnknownFields</span>

This setting will default to <span style="color:green">false</span>. Clear documentation will be provided, including a warning that enabling this feature may lead to data loss, as unknown fields in records will be silently dropped during ingestion.

The new configuration will apply to both ingestion modes (insertAll and Storage Write API), as both already support ignoring unknown fields via their respective client libraries ([insertAll documentation](https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/com.google.cloud.bigquery.InsertAllRequest#com_google_cloud_bigquery_InsertAllRequest_ignoreUnknownValues__), [Storage Write API documentation](https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/3.5.0/com.google.cloud.bigquery.storage.v1.JsonStreamWriter.Builder#com_google_cloud_bigquery_storage_v1_JsonStreamWriter_Builder_setIgnoreUnknownFields_boolean_)). The following will be the behaviour:

-   When <span style="color:green">ignoreUnknownFields</span> = <span style="color:green">true</span>, extra fields in messages will be dropped. To do so:

    -   Storage Write API:  <span style="color:green">JsonStreamWriter</span> will be created with <span style="color:green">.setIgnoreUnknownFields(true)</span>.

    -   insertAll:  <span style="color:green">InsertAllRequest</span> will be built with <span style="color:green">.setIgnoreUnknownValues(true)</span>.

-   When <span style="color:green">ignoreUnknownFields</span> = <span style="color:green">false</span>, strict schema alignment will be preserved. To do so:

    -   Storage Write API:  <span style="color:green">JsonStreamWriter</span> will be created with <span style="color:green">.setIgnoreUnknownFields(false)</span>.

    -   insertAll:  <span style="color:green">InsertAllRequest</span> will be built with <span style="color:green">.setIgnoreUnknownValues(false)</span>

#### Elaboration on schema compatibility with ignoreUnknownFields

When the flag is enabled, any fields in the record that are not present in the target BigQuery table schema are silently dropped by the client library. The behaviour then depends on the remaining schema fields in the table:

Case 1: Table has a nullable column "f1" with no default value defined, message contains only field "extra" (no overlap between schemas):

-   The writer silently drops the field "extra" (this is the only change in behaviour due to the new config).

-   When processing the JSON object, the writer sees "f1" is missing. The client library by default populates "f1" with NULL and sends it to BigQuery.

-   Result: since "f1" is nullable, the write succeeds but the row contains NULL in the "f1" column.

Case 2: Table has nullable column "f1" with a default value defined, message contains only field "extra" (no overlap between schemas):

-   The writer silently drops the field "extra" (this is the only change in behaviour due to the new config).

-   No value is once again found for "f1".

-   Although f1 has a default value defined in the schema, the client libraries will insert NULL value for fields that are not present (see NOTE on <span style="color:green">setDefaultMissingValueInterpretation</span> below for more discussion).

Case 3: Table has a required field "f1", message contains only field "extra"

-   The writer silently drops the field "extra" (this is the only change in behaviour due to the new config).

-   When processing JSON, the writer will find no value for "f1" and will attempt to use null. Request to BigQuery will fail with INVALID_ARGUMENT.

[NOTE] To respect defaults (E.g, Case 2), a separate option has to be enabled on the <span style="color:green">JsonStreamWriter</span>:

    .setDefaultMissingValueInterpretation(AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE)

Note: there does not seem to be an equivalent for InsertAll. This behavior can be added in future as a separate option, even possibly as the default behavior. 

#### SUMMARY

-   Unknown fields are always dropped silently

-   A field with the same name but different type is considered unknown and will be dropped.

-   For fields missing from the message:

    -   If they are nullable, they will be set to NULL.

    -   If they are nullable with default, NULL will still be sent (defaults will only be applied if this behaviour is explicitly enabled).

    -   If they are required, NULL will be sent, request will be rejected.