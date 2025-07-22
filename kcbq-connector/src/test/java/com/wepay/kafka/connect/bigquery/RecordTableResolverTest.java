package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.*;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.write.batch.MergeBatches;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordTableResolverTest {
    private static final String TOPIC = "topic";
    private static final String DATASET = "dataset";
    private static final String PROJECT = "project";
    private static final TableId BASE_TABLE_ID = TableId.of("dataset", "topic");
    private BigQuerySinkTaskConfig mockConfig;
    private BigQuery mockBigQuery;
    private MergeBatches mockMergeBatches;
    private SinkRecord mockRecord;
    private RecordTableResolver recordTableResolver;

    @BeforeEach
    public void setUp() {
        mockBigQuery = mock(BigQuery.class);
        mockMergeBatches = mock(MergeBatches.class);

        mockRecord = mock(SinkRecord.class);
        when(mockRecord.topic()).thenReturn(TOPIC);
        when(mockRecord.timestampType()).thenReturn(TimestampType.CREATE_TIME);

        mockConfig = mock(BigQuerySinkTaskConfig.class);
        when(mockConfig.getString(BigQuerySinkConfig.PROJECT_CONFIG)).thenReturn(PROJECT);
        when(mockConfig.getString(BigQuerySinkConfig.TOPICS_CONFIG)).thenReturn(TOPIC);
        when(mockConfig.getString(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG)).thenReturn(DATASET);
        when(mockConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(false);
        when(mockConfig.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(false);
        when(mockConfig.getBoolean(BigQuerySinkConfig.DELETE_ENABLED_CONFIG)).thenReturn(false);
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(false);
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG)).thenReturn(false);
    }

    @Test
    public void testResolvesTable() {
        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals(TOPIC, tableId.getTable());
        assertNull(tableId.getProject());
    }

    @Test
    public void testUpsertDeleteResolvesIntermediateTable() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);

        String intermediateTable = "intermediate_topic";
        TableId intermediateTableId = TableId.of(DATASET, intermediateTable);
        when(mockMergeBatches.intermediateTableFor(BASE_TABLE_ID)).thenReturn(intermediateTableId);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals(intermediateTable, tableId.getTable());
    }

    @Test
    public void testTimePartitioningOnNowTime() {
        String expectedTableName = TOPIC + "$" + LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);

        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        mockTableWithPartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals(expectedTableName, tableId.getTable());
    }

    @Test
    public void testPartitioningOnMessageTime() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG)).thenReturn(true);
        when(mockRecord.timestamp()).thenReturn(1720000000000L);
        mockTableWithPartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals("topic$20240703", tableId.getTable());
    }

    // tables should resolve successfully if BigQuery table is missing,
    // table creation will be later handled via Schema Manager if autoCreate tables is enabled
    @Test
    public void testResolvePartitionedTableWhenTableIsMissing() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        when(mockBigQuery.getTable(BASE_TABLE_ID)).thenReturn(null);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        String expectedSuffix = LocalDate.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals("topic$" + expectedSuffix, tableId.getTable());
    }

    @Test
    public void testPartitioningOnMessageTimeMissingTimestampType() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_MESSAGE_TIME_PARTITIONING_CONFIG)).thenReturn(true);
        when(mockRecord.timestampType()).thenReturn(TimestampType.NO_TIMESTAMP_TYPE);
        mockTableWithPartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);

        assertThrows(ConnectException.class, () -> recordTableResolver.getRecordTable(mockRecord));
    }

    @Test
    public void testResolvesPartitionedTableWhenTableHasNoPartitioning() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        mockTableWithPartitioning(null);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);

        assertThrows(ConnectException.class, () -> recordTableResolver.getRecordTable(mockRecord));
    }

    @ParameterizedTest
    @EnumSource(
            value = TimePartitioning.Type.class,
            names = {"HOUR", "MONTH", "YEAR"},
            mode = EnumSource.Mode.INCLUDE
    )
    public void testPartitioningThrowsWhenTablePartitioningNotByDay(TimePartitioning.Type invalidType) {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);
        mockTableWithPartitioning(TimePartitioning.of(invalidType));

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);

        ConnectException exception = assertThrows(
                ConnectException.class,
                () -> recordTableResolver.getRecordTable(mockRecord)
        );
        assertTrue(
                exception.getMessage().contains("partitioned by " + invalidType.name().toLowerCase() + " and not by day"),
                "Expected error message to indicate partitioning mismatch for " + invalidType
        );
    }

    @Test
    public void testPartitioningBigQueryAuthenticationException() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);

        BigQueryException authException = new BigQueryException(401, "Unauthorized");
        when(mockBigQuery.getTable(BASE_TABLE_ID)).thenThrow(authException);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);

        assertThrows(BigQueryException.class, () -> recordTableResolver.getRecordTable(mockRecord));
    }

    @Test
    public void testPartitioningBigQueryIoException() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.BIGQUERY_PARTITION_DECORATOR_CONFIG)).thenReturn(true);

        IOException ioCause = new IOException("Simulated IO error");
        BigQueryException ioException = new BigQueryException(0, "wrapped IO error", ioCause);
        when(mockBigQuery.getTable(BASE_TABLE_ID)).thenThrow(ioException);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);

        assertThrows(RetriableException.class, () -> recordTableResolver.getRecordTable(mockRecord));
    }

    @Test
    public void testResolveTableWithStorageApi() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals(TOPIC, tableId.getTable());
        assertEquals(PROJECT, tableId.getProject());
    }

    @Test
    public void testResolveTableWithStorageApiIgnoresUpsertDelete() {
        when(mockConfig.getBoolean(BigQuerySinkConfig.UPSERT_ENABLED_CONFIG)).thenReturn(true);
        when(mockConfig.getBoolean(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG)).thenReturn(true);

        String intermediateTable = "intermediate_topic";
        TableId intermediateTableId = TableId.of(DATASET, intermediateTable);
        when(mockMergeBatches.intermediateTableFor(BASE_TABLE_ID)).thenReturn(intermediateTableId);

        recordTableResolver = new RecordTableResolver(mockConfig, mockMergeBatches, mockBigQuery);
        TableId tableId = recordTableResolver.getRecordTable(mockRecord).getFullTableId();

        assertEquals(TOPIC, tableId.getTable());
    }

    private void mockTableWithPartitioning(TimePartitioning timePartitioning) {
        Table table = mock(Table.class);
        StandardTableDefinition tableDefinition = mock(StandardTableDefinition.class);
        when(table.getDefinition()).thenReturn(tableDefinition);
        when(tableDefinition.getTimePartitioning()).thenReturn(timePartitioning);
        when(mockBigQuery.getTable(BASE_TABLE_ID)).thenReturn(table);
    }
}