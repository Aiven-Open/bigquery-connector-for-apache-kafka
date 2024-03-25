package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQueryError;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ErrantRecordHandlerTest {

  @Test
  public void shouldReturnTrueOnAllowedBigQueryReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("invalid", "location", "message", "info"));

    // should allow sending records to dlq for bigquery reason:invalid (present in
    // allowedBigQueryErrorReason list)
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    assertTrue(expected);
  }

  @Test
  public void shouldReturnFalseOnNonAllowedReason() {
    ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(null);
    List<BigQueryError> bqErrorList = new ArrayList<>();
    bqErrorList.add(new BigQueryError("backendError", "location", "message", "info"));

    // Should not allow sending records to dlq for reason not present in
    // allowedBigQueryErrorReason list
    boolean expected = errantRecordHandler.isErrorReasonAllowed(bqErrorList);
    assertFalse(expected);
  }
}
