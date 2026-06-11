# Avro Temporal Logical Types Migration Guide

## Background

Starting from this version, the connector correctly maps Avro temporal logical
types to their corresponding BigQuery column types instead of falling through
to plain `INTEGER`.

This change requires a compatible version of the Confluent Schema Registry Avro
converter that produces named Connect schemas for these types (see
[confluentinc/schema-registry#4341](https://github.com/confluentinc/schema-registry/pull/4341)).
Both components must be upgraded together.

### Affected Avro logical types

| Avro logical type        | Previous BigQuery type | New BigQuery type |
|--------------------------|------------------------|-------------------|
| `timestamp-micros`       | `INTEGER`              | `TIMESTAMP`       |
| `timestamp-nanos`        | `INTEGER`              | `TIMESTAMP`       |
| `time-micros`            | `INTEGER`              | `TIME`            |
| `local-timestamp-millis` | `INTEGER`              | `DATETIME`        |
| `local-timestamp-micros` | `INTEGER`              | `DATETIME`        |
| `local-timestamp-nanos`  | `INTEGER`              | `DATETIME`        |

## Who is affected

You are affected if **all** of the following are true:

- Your Avro schemas use any of the logical types listed above
- You have existing BigQuery tables where these fields were previously written
  as `INTEGER`
- You are upgrading both the Avro converter and this connector simultaneously

If your tables were created after upgrading, or these field types were not
present in your schemas, no action is needed.

## Migration path

BigQuery does not support changing a column's type in place. The recommended
approach is to rename the old `INTEGER` column out of the way, recreate it with
the correct type, and backfill — so the connector keeps writing to the same
field name with no configuration changes required.

Repeat the following steps for each affected column in your table.

### Step 1 — Rename the old column

```sql
ALTER TABLE `my_dataset.my_table`
RENAME COLUMN <column> TO <column>_legacy;
```

### Step 2 — Add a new column with the correct type

Use the backfill expressions and target types from the table below:

| Avro logical type        | New BigQuery type | Backfill expression                              |
|--------------------------|-------------------|--------------------------------------------------|
| `timestamp-micros`       | `TIMESTAMP`       | `TIMESTAMP_MICROS(<column>_legacy)`              |
| `timestamp-nanos`        | `TIMESTAMP`       | `TIMESTAMP_MICROS(<column>_legacy / 1000)`       |
| `time-micros`            | `TIME`            | `TIME_ADD(TIME '00:00:00', INTERVAL <column>_legacy MICROSECOND)` |
| `local-timestamp-millis` | `DATETIME`        | `DATETIME(TIMESTAMP_MILLIS(<column>_legacy))`    |
| `local-timestamp-micros` | `DATETIME`        | `DATETIME(TIMESTAMP_MICROS(<column>_legacy))`    |
| `local-timestamp-nanos`  | `DATETIME`        | `DATETIME(TIMESTAMP_MICROS(<column>_legacy / 1000))` |

```sql
ALTER TABLE `my_dataset.my_table`
ADD COLUMN <column> <NEW_TYPE>;
```

### Step 3 — Backfill

```sql
UPDATE `my_dataset.my_table`
SET <column> = <backfill expression>
WHERE TRUE;
```

For example, for a `timestamp-micros` column named `ts_micros`:

```sql
ALTER TABLE `my_dataset.my_table` RENAME COLUMN ts_micros TO ts_micros_legacy;
ALTER TABLE `my_dataset.my_table` ADD COLUMN ts_micros TIMESTAMP;
UPDATE `my_dataset.my_table` SET ts_micros = TIMESTAMP_MICROS(ts_micros_legacy) WHERE TRUE;
```

### Step 4 — Upgrade

Upgrade the Avro converter and the connector. The connector will resume writing
to the original column names automatically — no topic routing or schema changes
needed.

### Step 5 — Drop the legacy columns

Once you have verified the migrated data, drop the temporary columns:

```sql
ALTER TABLE `my_dataset.my_table` DROP COLUMN <column>_legacy;
```

## Rollback

If you need to roll back after upgrading, you must reverse the table changes
as well as downgrade the software — the old connector cannot write raw
`INTEGER` values into `TIMESTAMP`, `DATETIME`, or `TIME` columns.

### Step 1 — Downgrade the connector and Avro converter

Restore the previous versions of both components before making any table
changes, so no new records land in the wrong column type.

### Step 2 — Restore the original INTEGER columns

For each migrated column, rename the new typed column out of the way and
restore the legacy `INTEGER` column to its original name:

```sql
ALTER TABLE `my_dataset.my_table` RENAME COLUMN <column> TO <column>_migrated;
ALTER TABLE `my_dataset.my_table` RENAME COLUMN <column>_legacy TO <column>;
```

For example, for `ts_micros`:

```sql
ALTER TABLE `my_dataset.my_table` RENAME COLUMN ts_micros TO ts_micros_migrated;
ALTER TABLE `my_dataset.my_table` RENAME COLUMN ts_micros_legacy TO ts_micros;
```

The connector will resume writing raw `INTEGER` values to the restored columns.

### Step 3 — Clean up

Once you have confirmed the rollback is stable, drop the migrated columns:

```sql
ALTER TABLE `my_dataset.my_table` DROP COLUMN <column>_migrated;
```
