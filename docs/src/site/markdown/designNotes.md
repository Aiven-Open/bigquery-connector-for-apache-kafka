# Design Notes

The BigQuery sink connector supports two distinct paths for inserting data into BigQuery.  The original BatchLoader path is uses GCS to store intermediate files before writing them to tables in BigQuery.  The second path is to use the StorageWriteAPI to stream the data to BigQuery.

## 