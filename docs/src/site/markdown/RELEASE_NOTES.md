
# Release Notes

All releases can be found at https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/releases

## v2.12.0
### What's changed
- upgraded to google-utils v1.1.0 and system v1.1.0


### Co-authored by
- Claude Warren

### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.11.0...v2.12.0

## v2.11.0
### What's changed
- Fix for CVE-2026-23529
- Updated versions
    - Java 8 -> 17
    - confluent 7.6.0 -> 7.9.5
    - jackson 2.14.2 -> 2.20.1
    - kafka 3.6.1 -> 3.8.1
- Introducted
    - aiven-commons:system:1.0.0
    - aiven-commons:google-utils:1.0.0
- updated README


### Co-authored by
- Aindriú Lavelle
- Audrey Budryte
- Claude Warren


### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.10.3...v2.11.0


## v2.10.3
### What's changed
 - fix: populate LogicalConverterRegistry in Task

### Co-authored by
 - Claude Warren


### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.10.2...v2.10.3

## v2.10.2
### What's changed
- Fix serializing Infinity/NaN for Floats when convertDoubleSpecialValues is enabled (#162)
- Fix bug with SchemaManager causing records to be placed on DLQ (#163)
- Update documentation for DECIMAL_HANDLING_MODE_CONFIG (#156)

### Co-authored by
- Aindriú Lavelle
- Claude Warren
- Erik Gustafsson
- HieuNT
- Ryan Skraba


### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.10.1...v2.10.2

## v2.10.1
### What's changed
- Fix potential silent data loss caused by stale BigQuery load job status not being refreshed (#155)
- Fix data loss from BigQuery write exception during GCSBatchTableWriter run (#153)
- Fix documentation error: the comments were attached to the wrong functions (#150)
- Bump version to 2.11.0-SNAPSHOT

### Co-authored by
- Claude Warren
- Gyllsdorff
- Pammi-Jyothi
- Ryan Skraba

### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.10.0...v2.10.1

## v2.10.0
### What's changed
- Add ability to ignore unknown fields in kafka messages (#88)
- skip test on kcbq-api build (#142)
- Enable partition decorator syntax for Storage Write API (#69)
- Specify Trace ID to enable BigQuery to track traffic originating from this connector. (#141)
- Add ByteBuffer TypeAdapter to avoid reflection crash on Java 9+ (#140)
- Add feature to update table schemas for GCStoBQ load jobs (#134)
- Enable connection pooling on Storage Write API Default Stream (#89)
- Update contributors list (#138)
- Add design doc for WriteAPI partition decorator support (#135)
- Add design doc and best practice doc (#136)
- Add since tags to some configuration options
- Update project id of BQ target table when useCredentialsProjectId is enabled (#123)
- Bump version to 2.10.0-SNAPSHOT

### Co-authored by
- Aindriú Lavelle
- Claude Warren
- hasan-cosan
- Mariia Podgaietska
- minsungoh
- Siddharth Agrawal
- Veli Can Ünal

### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.9.0...v2.10.0

## v2.9.0
### What's changed
- Fix retry login in GcsToBqWriter (#112)
- Update readme (#115)
- Create and Deploy the website (#102) (#110)
- Revert GCS Batch loading deprecation
- Add "deprecation" and "since" data to configuration options. (#103)
- Add ability to generate a documentation site. (#102)
- Enforced a minimum of 10 seconds for merge interval (#107)
- Add checks for BigQuery ingestion failures (#99)
- Revert deprecating partition decorator syntax (#68)
- Fix closing writers race condition (#98)
- Allow opt-in to use original message metadata (#97)
- Enable client-side request level retries for Storage Write API (#81)

### Co-authored by
- Brahmesh
- Claude Warren
- Davide Armand
- hasan-cosan
- Mariia Podgaietska

### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.8.0...v2.9.0

## v2.8.0
### What's Changed
- Updated Kafka.Decimal and Debezium.VariableScaleDecimal processing and adjusted tests (#82)
- Added methods to LogicalConverterRegistry (#79)
- Manifest contains valid specification information. #58 (#74)
- Converted LogicalConverter libraries to true utility classes (#78)
- Added log warning on lookup retries (#83)
- Use the project defined in the connector configuration when useCredentialsProjectId is set to true (#75)
- Retain the original topic, partition and offset in Kafka metadata record (#70)

### New Contributors
* @SamoylovMD made their first contribution in https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/70
* @hpmouton made their first contribution in https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/74

### Full Changelog ##
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.7.0...v2.8.0

---------
### Co-authored-by ##
 - Ryan Skraba <ryan.skraba@aiven.io>
 - Maksim Samoilov <maxim.samoilov@team.wrike.com>
 - Mouton <HoutonH@bdv.local>
 - veliuenal <veli.uenal@deliveryhero.com>

## v2.7.0

### What's Changed

* Deprecate GCS batch loading feature (#20)
* Deprecate partition decorator syntax feature (#21)
* Bumps org.eclipse.jetty:jetty-server from 9.4.53.v20231009 to 9.4.56.v20240826.
* Bumps org.eclipse.jetty:jetty-servlets from 9.4.53.v20231009 to 9.4.54.v20240208.
* Add header provider (#57)
* Retry getTable process before rasing any failure (#51)
* Enable bigdecimal conversion for Debezium numeric types (#41)
* Add optional configuration flag to use project ID from keyfile (#38)

### New Contributors
* @gharris1727 made their first contribution in https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/28
* @jeqo made their first contribution in https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/29
* @veliuenal made their first contribution in https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/pull/38

### Full Changelog
https://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v2.6.0...v2.7.0

## v2.6.0
### What's Changed
- Adds opt-in support for the [Storage Write API](https://cloud.google.com/bigquery/docs/write-api), which can be enabled via the `useStorageWriteApi` property. If enabled, the [default stream](https://cloud.google.com/bigquery/docs/write-api-streaming#at-least-once) will be used, unless the `enableBatchMode` property is set to `true`, in which case, the [pending type stream](https://cloud.google.com/bigquery/docs/write-api-batch#batch_load_data_using_pending_type) will be used instead. **NOTE: This is currently a beta feature and is not recommended for use in production.**
- Permits multiple topics to be routed to the same table with the `topic2TableMap` property (https://github.com/confluentinc/kafka-connect-bigquery/pull/361)
- Adds retry logic for "jobInternalError" errors when performing merge queries in upsert/delete mode (https://github.com/confluentinc/kafka-connect-bigquery/pull/363)
