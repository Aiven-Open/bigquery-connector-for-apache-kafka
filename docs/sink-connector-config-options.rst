=============================================
BigQuery Sink Connector Configuration Options
=============================================

``defaultDataset``
  The default dataset to be used

  * Type: string
  * Importance: high

``project``
  The BigQuery project to write to

  * Type: string
  * Importance: high

``autoCreateTables``
  Automatically create BigQuery tables if they don't already exist

  * Type: boolean
  * Default: true
  * Importance: high

``bigQueryMessageTimePartitioning``
  Whether or not to use the message time when inserting records. Default uses the connector processing time.

  * Type: boolean
  * Default: false
  * Importance: high

``bigQueryPartitionDecorator``
  Whether or not to append partition decorator to BigQuery table name when inserting records. Default is true. Setting this to true appends partition decorator to table name (e.g. table$yyyyMMdd depending on the configuration set for bigQueryPartitionDecorator). Setting this to false bypasses the logic to append the partition decorator and uses raw table name for inserts.

  * Type: boolean
  * Default: true
  * Importance: high

``gcsBucketName``
  The name of the bucket in which gcs blobs used to batch load to BigQuery should be located. Only relevant if enableBatchLoad is configured.

  * Type: string
  * Default: ""
  * Importance: high

``queueSize``
  The maximum size (or -1 for no maximum size) of the worker queue for bigQuery write requests before all topics are paused. This is a soft limit; the size of the queue can go over this before topics are paused. All topics will be resumed once a flush is requested or the size of the queue drops under half of the maximum size.

  * Type: long
  * Default: -1
  * Valid Values: [-1,...]
  * Importance: high

``allowBigQueryRequiredFieldRelaxation``
  If true, fields in BigQuery Schema can be changed from REQUIRED to NULLABLE

  * Type: boolean
  * Default: false
  * Importance: medium

``allowNewBigQueryFields``
  If true, new fields can be added to BigQuery tables during subsequent schema updates

  * Type: boolean
  * Default: false
  * Importance: medium

``allowSchemaUnionization``
  If true, the existing table schema (if one is present) will be unionized with new record schemas during schema updates

  * Type: boolean
  * Default: false
  * Importance: medium

``autoCreateBucket``
  Whether to automatically create the given bucket, if it does not exist. Only relevant if enableBatchLoad is configured.

  * Type: boolean
  * Default: true
  * Importance: medium

``bigQueryRetry``
  The number of retry attempts that will be made per BigQuery request that fails with a backend error or a quota exceeded error

  * Type: int
  * Default: 0
  * Valid Values: [0,...]
  * Importance: medium

``bigQueryRetryWait``
  The minimum amount of time, in milliseconds, to wait between BigQuery backend or quota exceeded error retry attempts.

  * Type: long
  * Default: 1000
  * Valid Values: [0,...]
  * Importance: medium

``gcsFolderName``
  The name of the folder under the bucket in which gcs blobs used to batch load to BigQuery should be located. Only relevant if enableBatchLoad is configured.

  * Type: string
  * Default: ""
  * Importance: medium

``keySource``
  Determines whether the keyfile config is the path to the credentials json file or the raw json of the key itself. If set to APPLICATION_DEFAULT, the keyfile should not be provided and the connector will use any GCP application default credentials that it can find on the Connect worker for authentication.

  * Type: string
  * Default: FILE
  * Valid Values: [FILE, JSON, APPLICATION_DEFAULT]
  * Importance: medium

``keyfile``
  The file containing a JSON key with BigQuery service account credentials

  * Type: password
  * Default: null
  * Importance: medium

``max.retries``
  The maximum number of times to retry on retriable errors before failing the task.

  * Type: int
  * Default: 10
  * Valid Values: [1,...]
  * Importance: medium

``sanitizeFieldNames``
  Whether to automatically sanitize field names before using them as field names in big query. Big query specifies that field name can only contain letters, numbers, and underscores. The sanitizer will replace the invalid symbols with underscore. If the field name starts with a digit, the sanitizer will add an underscore in front of field name. Note: field a.b and a_b will have same value after sanitizing, and might cause key duplication error.

  * Type: boolean
  * Default: false
  * Importance: medium

``sanitizeTopics``
  Whether to automatically sanitize topic names before using them as table names; if not enabled topic names will be used directly as table names

  * Type: boolean
  * Default: false
  * Importance: medium

``schemaRetriever``
  A class that can be used for automatically creating tables and/or updating schemas

  * Type: class
  * Default: com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever
  * Importance: medium

``threadPoolSize``
  The size of the BigQuery write thread pool. This establishes the maximum number of concurrent writes to BigQuery.

  * Type: int
  * Default: 10
  * Valid Values: [1,...]
  * Importance: medium

``useStorageWriteApi``
  (Beta feature: use with caution) Use Google's New Storage Write API for data streaming. Not available for upsert/delete mode

  * Type: boolean
  * Default: false
  * Importance: medium

``useProjectFromCreds``
  Use the ``quotaProjectId`` from the Google credentials.

  * Type: boolean
  * Default: false
  * Importance: low

``allBQFieldsNullable``
  If true, no fields in any produced BigQuery schema will be REQUIRED. All non-nullable avro fields will be translated as NULLABLE (or REPEATED, if arrays).

  * Type: boolean
  * Default: false
  * Importance: low

``avroDataCacheSize``
  The size of the cache to use when converting schemas from Avro to Kafka Connect

  * Type: int
  * Default: 100
  * Valid Values: [0,...]
  * Importance: low

``batchLoadIntervalSec``
  The interval, in seconds, in which to attempt to run GCS to BQ load jobs. Only relevant if enableBatchLoad is configured.

  * Type: int
  * Default: 120
  * Importance: low

``clusteringPartitionFieldNames``
  List of fields on which data should be clustered by in BigQuery, separated by commas

  * Type: list
  * Default: null
  * Valid Values: com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig$$Lambda/0x000000f001235108@5d9f3e38
  * Importance: low

``commitInterval``
  The interval, in seconds, in which to attempt to commit streamed records.

  * Type: int
  * Default: 60
  * Valid Values: [15,...,14400]
  * Importance: low

``convertDoubleSpecialValues``
  Should +Infinity be converted to Double.MAX_VALUE and -Infinity and NaN be converted to Double.MIN_VALUE so they can make it to BigQuery

  * Type: boolean
  * Default: false
  * Importance: low

``deleteEnabled``
  Enable delete functionality on the connector through the use of record keys, intermediate tables, and periodic merge flushes. A delete will be performed when a record with a null value (i.e., a tombstone record) is read.

  * Type: boolean
  * Default: false
  * Importance: low

``enableBatchLoad``
  Beta Feature; use with caution: The sublist of topics to be batch loaded through GCS

  * Type: list
  * Default: ""
  * Importance: low

``enableBatchMode``
  Use Google's New Storage Write API with batch mode

  * Type: boolean
  * Default: false
  * Importance: low

``intermediateTableSuffix``
  A suffix that will be appended to the names of destination tables to create the names for the corresponding intermediate tables. Multiple intermediate tables may be created for a single destination table, but their names will always start with the name of the destination table, followed by this suffix, and possibly followed by an additional suffix.

  * Type: string
  * Default: tmp
  * Valid Values: non-empty string
  * Importance: low

``kafkaDataFieldName``
  The name of the field of Kafka Data. Default to be null, which means Kafka Data Field will not be included. 

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: low

``kafkaKeyFieldName``
  The name of the field of Kafka key. Default to be null, which means Kafka Key Field will not be included.

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: low

``mergeIntervalMs``
  How often (in milliseconds) to perform a merge flush, if upsert/delete is enabled. Can be set to -1 to disable periodic flushing. Either mergeIntervalMs or mergeRecordsThreshold, or both must be enabled.

  This should not be set to less than 10 seconds. A validation would be introduced in a future release to this effect.

  * Type: long
  * Default: 60000
  * Valid Values: Either a positive integer or -1 to disable time interval-based merging
  * Importance: low

``mergeRecordsThreshold``
  How many records to write to an intermediate table before performing a merge flush, if upsert/delete is enabled. Can be set to -1 to disable record count-based flushing. Either mergeIntervalMs or mergeRecordsThreshold, or both must be enabled.

  * Type: long
  * Default: -1
  * Valid Values: Either a positive integer or -1 to disable throughput-based merging
  * Importance: low

``partitionExpirationMs``
  The amount of time, in milliseconds, after which partitions should be deleted from the tables this connector creates. If this field is set, all data in partitions in this connector's tables that are older than the specified partition expiration time will be permanently deleted. Existing tables will not be altered to use this partition expiration time.

  * Type: long
  * Default: null
  * Valid Values: com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig$$Lambda/0x000000f001235558@16147134
  * Importance: low

``timestampPartitionFieldName``
  The name of the field in the value that contains the timestamp to partition by in BigQuery and enable timestamp partitioning for each table. Leave this configuration blank, to enable ingestion time partitioning for each table.

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: low

``topic2TableMap``
  Map of topics to tables (optional). Format: comma-separated tuples, e.g. <topic-1>:<table-1>,<topic-2>:<table-2>,... Note that topic name should not be modified using regex SMT while using this option.Also note that SANITIZE_TOPICS_CONFIG would be ignored if this config is set.Lastly, if the topic2table map doesn't contain the topic for a record, a table with the same name as the topic name would be created

  * Type: string
  * Default: ""
  * Valid Values: com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig$$Lambda/0x000000f001234000@69a3b76d
  * Importance: low

``upsertEnabled``
  Enable upsert functionality on the connector through the use of record keys, intermediate tables, and periodic merge flushes. Row-matching will be performed based on the contents of record keys.

  * Type: boolean
  * Default: false
  * Importance: low

Common
^^^^^^

``topics``
  List of topics to consume, separated by commas

  * Type: list
  * Default: ""
  * Importance: high

``topics.regex``
  Regular expression giving topics to consume. Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. Only one of topics or topics.regex should be specified.

  * Type: string
  * Default: ""
  * Importance: high




``timePartitioningType``
  The time partitioning type to use when creating tables, or 'NONE' to create non-partitioned tables. Existing tables will not be altered to use this partitioning type.

  * Type: string
  * Default: DAY
  * Valid Values: com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig$$Lambda/0x000000f001239e90@356a0fbe
  * Importance: low


