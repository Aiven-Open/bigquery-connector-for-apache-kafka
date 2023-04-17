package com.wepay.kafka.connect.bigquery.exception;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BigQueryStorageWriteApiErrorResponsesTest {

    @Test
    public void testTableMissingDueToPermissionDenied(){
        String message = "PERMISSION_DENIED on resource table abc (or it may not exist)";
        boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
        assertTrue(result);
    }

    @Test
    public void testTableMissingDueToNotFound(){
        String message = "Not found: table abc";
        boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
        assertTrue(result);
    }

    @Test
    public void testTableMissingDueToDeleted(){
        String message = "Not found or Table is deleted";
        boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
        assertTrue(result);
    }

    @Test
    public void testTableNotMissing(){
        String message = "INTERNAL: internal error occurred";
        boolean result = BigQueryStorageWriteApiErrorResponses.isTableMissing(message);
        assertFalse(result);
    }

    @Test
    public void testRetriableInternal(){
        String message = "INTERNAL: internal error occurred";
        boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
        assertTrue(result);
    }

    @Test
    public void testRetriableAborted(){
        String message = "ABORTED: operation is aborted";
        boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
        assertTrue(result);
    }

    @Test
    public void testRetriableCancelled(){
        String message = "CANCELLED: stream cancelled on user action";
        boolean result = BigQueryStorageWriteApiErrorResponses.isRetriableError(message);
        assertTrue(result);
    }

    @Test
    public void testMalformedRequest(){
        Map<Integer, String> errors = new HashMap<>();
        errors.put(0, "JSONObject has fields unknown to BigQuery: root.f1.");
        String message = "INVALID_ARGUMENT:  JSONObject has fields unknown to BigQuery: root.f1.";
        Exceptions.AppendSerializtionError error = new Exceptions.AppendSerializtionError(
                3,
                message,
                "DEFAULT",
                errors);
        boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(error);
        assertTrue(result);
    }

    @Test
    public void testNonInvalidArgument(){
        Map<Integer, String> errors = new HashMap<>();
        String message = "Deadline Exceeded";
        Exceptions.AppendSerializtionError error = new Exceptions.AppendSerializtionError(
                13,
                message,
                "DEFAULT",
                errors);
        boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(new Exception(error));
        assertFalse(result);
    }

    @Test
    public void testNonMalformedException(){
        String message = "Deadline Exceeded";
        Exception e= new Exception(message);
        boolean result = BigQueryStorageWriteApiErrorResponses.isMalformedRequest(e);
        assertFalse(result);
    }
}
