# **BQ Sink Connector Partition Decorator Support for Write API**

Author: [Mariia Podgaietska](mailto:mpodgaietska@google.com) | Date: Jul 9, 2025

## **Objective**

Enable support for Storage Write API Partition Decorator syntax in the BigQuery Sink Connector ([Aiven-Open/bigquery-connector-for-apache-kafka](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka)).

## **Background**

The BigQuery Sink Connector currently supports Partition Decorator syntax when using the legacy insertAll API. This syntax allows writing records directly into specific partitions of a BigQuery table by appending a <span style="color:green">$YYYYMMDD</span> suffix to the table name. However, when the connector is configured to use BigQuery Storage Write API (<span style="color:green">useStorageWriteApi=true</span>), this behaviour is not yet implemented/enabled.

Since the Storage Write Api now officially supports partition decorator syntax, we want to enable this feature in the connector. Doing so will bring better feature parity between two write paths (insertAll and writeApi), allowing for a more seamless migration from the legacy insertAll API.

### **Current Behaviour**

Since the connector already implements partition decorator syntax, it is important to first understand the current connector behavior for insertAll when use of partition decorator syntax is enabled and then look at what changes need to be made to enable the feature for StorageWriteApi.

In insertAll mode, the connector supports multiple ways to resolve the correct decorated table name (with <span style="color:green">$YYYYMMDD</span> suffix) based on a combination of already existing configurations ([official documentation](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/blob/main/docs/sink-connector-config-options.rst#:~:text=Importance%3A%20high-,bigQueryMessageTimePartitioning,Importance%3A%20high,-gcsBucketName)):

1. With <span style="color:green">bigQueryPartitionDecorator=true</span> (is true by default) and <span style="color:green">bigQueryMessageTimePartitioning=false</span>, the connector appends the current system time as the suffix to the table name.
2. With <span style="color:green">bigQueryPartitionDecorator=true</span> and <span style="color:green">bigQueryMessageTimePartitioning=true</span> the connector derives the partition from the record’s timestamp.

The timestamps used for partitioning (when <span style="color:green">bigQueryMessageTimePartitioning=true</span>) can be set in one of the following ways:

1. By Kafka Producer \- the default behaviour of kafka client libraries
2. By the Kafka Broker \- when kafka broker receives the message
3. Explicitly by the user \- when creating the ProducerRecord, can explicitly specify timestamp

Therefore, the partition decorator feature within the connector can provide a powerful way to write historical data to correct partitions or backfill data into BigQuery tables.

#### **Failure Scenarios**

Use of partition decorator syntax fails (on the connector side) in the following scenarios:

* **Table is not partitioned by DAY**: The use of decorator syntax requires the target table to be partitioned by DAY. If the table is partitioned by another type (e.g., MONTH, HOUR, etc.), the connector fails on first write with the following: *"Cannot use decorator syntax to write to table as it is partitioned by MONTH and not by day"*
* **Record is missing timestamp:** When <span style="color:green">bigQueryMessageTimePartitioning=true</span>, the connector expects each record to have a timestamp set. If a record’s timestamp is NO\_TIMESTAMP\_TYPE, the connector will not be able to determine the partition and fails with the following: *"Message has no timestamp type, cannot use message timestamp to partition."*

To enable partition decorator syntax with Storage Write API, we need to ensure we are maintaining the same failure scenarios.

### **Current Implementation**

For all records delivered to the put() method, the SinkTask determines the appropriate BigQuery table to write to. The resolution logic is currently split into two separate methods depending on whether the Storage Write API is enabled:

```java
BigQuerySinkTask.java

PartitionedTableId table = useStorageApi ? getStorageApiRecordTable(record.topic()) : getRecordTable(record);
```

Detailed look at the difference of table resolution implementation:

* getStorageWriteApiRecordTable:  
  Since partition decorator syntax is currently not supported with the Storage Write API, it is assumed that all records from the same topic map to the same BigQuery table. The table is resolved only once for every first record of a topic and cached to allow immediate return when subsequent records from an already processed topic come in.
* getRecordTable:  
  The table is re-resolved for **every** record. When <span style="color:green">usePartitionDecorator=true</span>, because even records from the same topic might need to be written to different table partitions depending on system time/ record timestamp. Caching PartitionedTableId in this case can lead to incorrect table resolution (e.g, reusing tables with stale partition suffixes).

  When partition decorator is enabled:
* The method retrieves the StandardTableDefinition from the BigQuery target table.
* Validates that it is partitioned by DAY (a requirement for partition decorator support).
* Set partition in PartitionedTableId and build to generate TableId with suffix <span style="color:green">$YYYYMMDD</span>.

  Additionally, getRecordTable also handles computation of intermediate tables when upsertDelete mode is enabled.

## **Implementation Adjustments**

#### **Re-introducing Partition Decorator Syntax**

As of version [2.7.0](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/60), the connector disabled support for partition decorator syntax altogether. This was based on the assumption that partition decorator syntax is not supported by Storage Write API, combined with the intent to eventually deprecate InsertAll API support (see the [following](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/issues/10)). To restore support for partition decorators, the deprecated warning will need to be removed.

#### **Enabling Partition Decorator Syntax in Storage Write Api mode**

Given that with the enablement of partition decorator for Storage Write Api most of the table resolution logic will be shared with the insertAll Api, we can consolidate the two existing methods (getStorageWriteApiRecordTable and  getRecordTable) into a unified getRecordTable method. This will avoid unnecessary code duplication and ensure consistent handling of partition decorators across both write paths.

The connector already enforces a constraint during startup that disallows enabling both useStorageWriteApi and upsertDelete simultaneously. As seen in [StorageWriteApiValidator](https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/blob/b1eb547916a678ab831711e598c620aa8465d3a6/kcbq-connector/src/main/java/com/wepay/kafka/connect/bigquery/config/StorageWriteApiValidator.java#L74):

```java
if (config.getBoolean(UPSERT_ENABLED_CONFIG)) {
      return Optional.of(upsertNotSupportedError);
} else if (config.getBoolean(DELETE_ENABLED_CONFIG)) {
        return Optional.of(deleteNotSupportedError);
}
```

This makes it safe for getRecordTable to contain logic specific to resolving intermediate tables for upsert/delete mode even when the connector is running with Storage Write Api enabled. However to safeguard it further, we can also enable the upsertDelete flag in BigQuerySinkTask only if the Storage Write API is disabled. Eg:

```java
BigQuerySinkTask.java

upsertDelete = !useStorageWriteApi && config.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG) || config.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG) 
```

#### **Optimization of Table Resolution**

The current implementation of getRecordTable performs unnecessary work, leading to inefficiency, especially for high-throughput topics. These issues will become more critical as getRecordTable will now be used by both StorageWriteApi path and insertAll, so will need to be addressed.

1. **Redundant Base Table ID Computation**: The current implementation recomputes the base table ID for every record. The base table ID is then used to derive the final table name (if upsert/delete or partition decorator is enabled) or is returned directly for standard flow. This re-computation, however, is redundant, since all records from a specific topic will always resolve to the same base table ID.
1. **Unnecessary Partition Validation**: When the usePartitionDecorator setting is enabled, the getRecordTable further validates that the table is partitioned by DAY (repeatedly for every record). This re-validation is also redundant because table partitioning in BigQuery is immutable once the table is created. It is, therefore, enough to verify table partitioning once per topic instead of for every individual record.

To address this redundant work, we can introduce a caching mechanism. The proposed approach is to:

* On the **first record** for a topic, retrieve the StandardTableDefinition from BigQuery, validate that the table is partitioned by day, and then cache the base table ID for that topic.
* For **all subsequent records** from the same topic, the function will retrieve the table ID directly from the cache. This bypasses the need to retrieve and revalidate the table definition, allowing the function to immediately apply the partition decorator syntax.

#### **Handling Decorated vs Undecorated Table Names in the Storage Write API Flow**

Currently, in the Storage Write API flow, the connector uses the undecorated (base) table name when constructing the StorageWriteApiWriter:

```java
BigQuerySinkTask.java

if (useStorageApi) {
    tableWriterBuilder = new StorageWriteApiWriter.Builder(
    storageApiWriter,
    TableNameUtils.tableName(table.getBaseTableId()),
    recordConverter,
    batchHandler
 );
}
```

This behaviour must be revised to support partition decorator syntax. However, we cannot simply replace the table.getBaseTableId() with table.getFullTableId() (the partition decorated name) as to maintain the existing behaviour, the stream writer also needs to be able to perform table operations (auto-creating or updating tables) via the StorageWriteApiRetryHandler if the connector is configured to do so:

```java
StorageWriteApiRetryHandler.java

public void attemptTableOperation(BiConsumer<TableId, List<SinkRecord>> tableOperation) {
    try {
      tableOperation.accept(tableId(), records);
      // Table takes time to be available for after creation
      setAdditionalRetriesAndWait();
    }
```

These table operations must use the base table name to avoid BigQuery API rejecting the requests with the following errors: *“Invalid table ID “table$20250710”. Table Ids must be alphanumeric (plus underscores) and must be at most 1024 characters long. Also, table decorators cannot be used”.*

To ensure correctness and avoid breaking table manipulation functionality, we need to ensure we are using correct table name representations within the Storage Write Api flow:

* The decorated table name must be used when creating and caching JsonStreamWriters in map (decoratedTableName to JsonStreamWriter)
* The base table name must be used for table-related operations (creations and updates).

#### **Accounting for Storage Write Api Partition Decorator Limitations**

Currently, BigQuery Storage Write API **only** supports partition decorator syntax with DEFAULT stream. It does not support decorators when using Application Streams, e.g, PENDING which is used for batch loading (when <span style="color:green">enableBatchMode=true</span>).

To make it explicit that use of partition decorator is not available when using batch mode, we will need to add to the validation logic in the StorageWriteApiValidator. On connector startup, we will return an error if the user explicitly requested partition decorator syntax and batch mode, or silently disable partition decorator otherwise.

## **Testing**

In order to validate the logic for enabling partition decorator syntax in useStorageApi mode, additional unit and integration tests will need to be introduced.

### **Unit Tests**

Additional unit tests will need to be added to ensure correctness of implementation:

* BigQuerySinkTaskTest: Test coverage will need to be expanded to verify tables are resolved correctly across configuration combinations of bigQueryPartitionDecorator and bigQueryMessageTimePartitioning. Additionally, we will verify that ConnectionException is correctly thrown for error cases outlined in *Failure Scenarios.*
* StorageWriteApiValidatorTest: test to ensure that an explicit error is returned when both bigQueryPartitionDecorator and enableBatchMode are enabled simultaneously.

### **Integration Tests**

In addition to unit tests, we’ll also want to create an integration test suite StorageWriteApiTimePartitioning. This will be testing the connector when it is configured to use both StorageWriteApi and partition decorator, and will need to verify the following:

1. Connector correctly routes records to expected partitions:
   * Records are written to today’s partition when <span style="color:green">bigQueryPartitionDecorator=true</span> and <span style="color:green">bigQueryMessageTimePartitioning=false</span>.
   * Records are written to correct partitions (days in the past and future) when <span style="color:green">bigQueryPartitionDecorator=true</span> and <span style="color:green">bigQueryMessageTimePartitioning=true</span>.
       * Will need to produce messages to the topic with explicitly set timestamps (eg. tomorrow, yesterday, etc.)
   * Can verify the scenarios with the following:

     SELECT \* FROM \`dataset.table\`

     WHERE \_PARTITIONTIME \=

     TIMESTAMP\_TRUNC(TIMESTAMP\_MILLIS(\<expected\_timestamp\_ms\>), ‘DAY’)

2. Connector maintains ability to create and update BigQuery tables when <span style="color:green">bigQueryPartitionDecorator=true</span> and connector is configured to do so (<span style="color:green">autoCreateTables=true</span>,  <span style="color:green">allowNewBigQueryFields=true</span>, <span style="color:green">allowBigQueryRequiredFieldRelaxation</span>, etc.)
