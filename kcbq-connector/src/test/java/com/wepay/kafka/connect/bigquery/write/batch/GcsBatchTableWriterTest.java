package com.wepay.kafka.connect.bigquery.write.batch;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.util.concurrent.Futures;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.row.GcsToBqWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GcsBatchTableWriterTest {
    private GcsBatchTableWriter underTest;
    private final String bucketName = "test-bucket";
    private final String baseBlobName = "testBlobName";
    private final TableId tableId = TableId.of("dataset", "table");


    @Test
    void badWriterTest() throws InterruptedException {


        // retries/wait are irrelevant when everything succeeds first try
        int retries = 3;
        long retryWaitMs = 100;
        boolean autoCreate = false;

        // throw an exception.
        BigQuery bigQuery = mock(BigQuery.class);
        when(bigQuery.getTable(any(TableId.class))).thenThrow(new BigQueryException(new IOException("testing exception")));

        Storage storage = mock(Storage.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        Time mockTime = new MockTime();

        GcsToBqWriter writer =
                new GcsToBqWriter(
                        storage, bigQuery, schemaManager, retries, retryWaitMs, autoCreate, false, mockTime);


        List<SinkRecord> records = new ArrayList<>();

        ErrantRecordReporter recordReporter = new ErrantRecordReporter() {
            Throwable t = null;
            @Override
            public Future<Void> report(SinkRecord record, Throwable error) {
                records.add(record);
                if (t == null) {
                    t = error;
                } else {
                    if (!t.equals(error)) {
                        fail("New error detected");
                    }
                }
                return Futures.immediateFuture(null);
            }
        };

        ErrantRecordHandler errantRecordHandler = new ErrantRecordHandler(recordReporter);

        InsertAllRequest.RowToInsert rowToInsert = mock(InsertAllRequest.RowToInsert.class);
        SinkRecordConverter recordConverter = mock(SinkRecordConverter.class);
        when(recordConverter.getRecordRow(any(SinkRecord.class), any(TableId.class))).thenReturn(rowToInsert);

        GcsBatchTableWriter.Builder builder = new GcsBatchTableWriter.Builder(writer, tableId, bucketName, baseBlobName, recordConverter, errantRecordHandler);

        for (int i =0; i < 10; i++) {
            SinkRecord sr = mock(SinkRecord.class);
            when(sr.kafkaPartition()).thenReturn(0);
            when(sr.kafkaOffset()).thenReturn((long) i);
            when(sr.key()).thenReturn("key" + i);
            builder.addRow(sr, tableId);
        }

        ConnectException exception = assertThrows(ConnectException.class, () -> builder.build().run());
        assertEquals("Failed to write rows to GCS", exception.getMessage());
        assertEquals(10, records.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, records.get(i).kafkaOffset());
        }
    }
}
