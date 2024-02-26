package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrantRecordHandler {
  private static final Logger logger = LoggerFactory.getLogger(ErrantRecordHandler.class);
  private static final List<String> allowedBigQueryErrorReason = Arrays.asList("invalid");
  private final ErrantRecordReporter errantRecordReporter;

  public ErrantRecordHandler(ErrantRecordReporter errantRecordReporter) {
    this.errantRecordReporter = errantRecordReporter;
  }

  public void reportErrantRecords(Set<SinkRecord> records, Exception e) {
    if (errantRecordReporter != null) {
      logger.debug("Sending {} records to DLQ", records.size());
      for (SinkRecord r : records) {
        // Reporting records in async mode
        errantRecordReporter.report(r, e);
      }
    } else {
      logger.warn("Cannot send Records to DLQ as ErrantRecordReporter is null");
    }
  }

  public void reportErrantRecords(Map<SinkRecord, Throwable> rowToError) {
    if (errantRecordReporter != null) {
      logger.debug("Sending {} records to DLQ", rowToError.size());
      for (Map.Entry<SinkRecord, Throwable> rowToErrorEntry : rowToError.entrySet()) {
        // Reporting records in async mode
        errantRecordReporter.report(rowToErrorEntry.getKey(), rowToErrorEntry.getValue());
      }
    } else {
      logger.warn("Cannot send Records to DLQ as ErrantRecordReporter is null");
    }
  }

  public ErrantRecordReporter getErrantRecordReporter() {
    return errantRecordReporter;
  }

  public boolean isErrorReasonAllowed(List<BigQueryError> bqErrorList) {
    for (BigQueryError bqError : bqErrorList) {
      boolean errorMatch = false;
      String bqErrorReason = bqError.getReason();
      for (String allowedBqErrorReason : allowedBigQueryErrorReason) {
        if (bqErrorReason.equalsIgnoreCase(allowedBqErrorReason)) {
          errorMatch = true;
          break;
        }
      }
      if (!errorMatch) {
        return false;
      }
    }
    return true;
  }
}
