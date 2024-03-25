package com.wepay.kafka.connect.bigquery.exception;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.BigQueryException;
import org.junit.jupiter.api.Test;

public class BigQueryErrorResponsesTest {

  @Test
  public void testIsAuthenticationError() {
    BigQueryException error = new BigQueryException(0, "......401.....Unauthorized error.....");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......401.....Unauthorized error...invalid_grant..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......400........invalid_grant..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......400.....invalid_request..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......400.....invalid_client..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......400.....unauthorized_client..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......400.....unsupported_grant_type..");
    assertTrue(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......403..Access denied error.....");
    assertFalse(BigQueryErrorResponses.isAuthenticationError(error));

    error = new BigQueryException(0, "......500...Internal Server Error...");
    assertFalse(BigQueryErrorResponses.isAuthenticationError(error));
  }
}
