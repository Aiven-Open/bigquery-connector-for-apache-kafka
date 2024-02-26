package com.wepay.kafka.connect.bigquery.write.storage;

/**
 * Enums for Stream states
 */
public enum StreamState {
  CREATED,
  APPEND,
  FINALISED,
  COMMITTED,
  INACTIVE
}
