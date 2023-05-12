package com.wepay.kafka.connect.bigquery.write.storageApi;

import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.protobuf.Descriptors;
import com.wepay.kafka.connect.bigquery.exception.BigQueryStorageWriteApiConnectException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Plain JAVA class with all utility methods on Application streams.
 * Streams which are created by calling application are called as Application streams
 * Uses stream writer methods from:
 * https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1
 */
public class ApplicationStream {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationStream.class);
    private StreamState currentState = null;
    private final String tableName;
    private WriteStream stream = null;
    private JsonStreamWriter jsonWriter = null;
    private final Map<TopicPartition, OffsetAndMetadata> offsetInformation;
    private final BigQueryWriteClient client;
    /**
     * Number of times append is called
     */
    private final AtomicInteger appendCalls;
    /**
     * Number of append requests completed successfully. This can never be greater than appendCalls
     */
    private final AtomicInteger completedCalls;

    /**
     * This is called by builder to capture maximum calls expected to append.
     */
    private final AtomicInteger maxCalls;

    public ApplicationStream(String tableName, BigQueryWriteClient client) throws Exception {
        this.client = client;
        this.tableName = tableName;
        this.offsetInformation = new HashMap<>();
        this.appendCalls = new AtomicInteger();
        this.maxCalls = new AtomicInteger();
        this.completedCalls = new AtomicInteger();
        generateStream();
        currentState = StreamState.CREATED;
    }

    public Map<TopicPartition, OffsetAndMetadata> getOffsetInformation() {
        return offsetInformation;
    }

    private void generateStream() throws Descriptors.DescriptorValidationException, IOException, InterruptedException {
        this.stream = client.createWriteStream(
                tableName, WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build());
        this.jsonWriter = JsonStreamWriter.newBuilder(stream.getName(), client).build();
    }

    public void closeStream() {
        if (!this.jsonWriter.isClosed()) {
            this.jsonWriter.close();
            logger.info("JSON Writer for stream {} closed", getStreamName());
        }
    }

    public String getStreamName() {
        return this.stream.getName();
    }

    /**
     * Increases the Append call count and returns the updated value
     */
    public void increaseAppendCall() {
        this.appendCalls.incrementAndGet();
    }

    /**
     * Increases the Max call count by 1. This tells the total expected calls which would be made to append method.
     * Returns the updated value
     */
    public int increaseMaxCalls() {
        int count = this.maxCalls.incrementAndGet();
        if (currentState == StreamState.CREATED) {
            currentState = StreamState.APPEND;
        }
        return count;
    }

    /**
     * Increases the count of Append calls which are completed.
     * Returns the updated value
     */
    public void increaseCompletedCalls() {
        this.completedCalls.incrementAndGet();
    }

    /**
     * Stream can be closed for writing (not appending new data) only if its current state is different from created
     * A stream with CREATED state tells the stream has not been used for writing anything and would result in resource
     * wastage we create new without using the existing one
     *
     * @return True if this stream can be marked as non-active(No new data would be assigned to it). Please note inactive is different
     * which means the stream has completed it lifecycle
     */
    public boolean canTransitionToNonActive() {
        return currentState != StreamState.CREATED;
    }

    /**
     * Updates offset handled by this particular stream. Each update offset call mean one batch of records that would
     * be sent to append
     *
     * @param offsets - New offsets to be added on top of existing
     */
    public void updateOffsetInformation(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsetInformation.putAll(offsets);
        increaseMaxCalls();
    }

    public JsonStreamWriter writer() {
        if (this.jsonWriter.isClosed()) {
            logger.debug("JSON Stream Writer is closed. Attempting to recreate stream and writer");
            synchronized (this) {
                resetStream();
            }
        }
        return this.jsonWriter;
    }

    /**
     * @return Returns true if all append calls are completed and the completed calls is equal to maximum calls with
     * this stream
     */
    public boolean areAllExpectedCallsCompleted() {
        return (this.maxCalls.intValue() == this.appendCalls.intValue())
                && (this.appendCalls.intValue() == this.completedCalls.intValue());
    }

    /**
     * Finalises the stream
     */
    public void finalise() {
        if (currentState == StreamState.APPEND) {
            FinalizeWriteStreamResponse finalizeResponse =
                    client.finalizeWriteStream(this.getStreamName());
            logger.debug("Rows written: " + finalizeResponse.getRowCount());
            currentState = StreamState.FINALISED;
        } else {
            throw new BigQueryStorageWriteApiConnectException(
                    "Stream could not be finalised as current state " + currentState + " is not expected state.");
        }
    }

    /**
     * Commits the finalised stream
     */
    public void commit() {
        if (currentState == StreamState.FINALISED) {
            BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                            .setParent(tableName)
                            .addWriteStreams(getStreamName())
                            .build();
            BatchCommitWriteStreamsResponse commitResponse = client.batchCommitWriteStreams(commitRequest);
            // If the response does not have a commit time, it means the commit operation failed.
            if (!commitResponse.hasCommitTime()) {
                // We are always sending 1 stream so at max should have just 1 error
                StorageError storageError = commitResponse.getStreamErrors(0);
                throw new BigQueryStorageWriteApiConnectException(
                        String.format("Failed to commit stream %s due to %s", getStreamName(), storageError));

            }
            logger.debug(
                    "Appended and committed records successfully for stream {} at {}",
                    getStreamName(),
                    commitResponse.getCommitTime());
            currentState = StreamState.COMMITTED;
        } else {
            throw new BigQueryStorageWriteApiConnectException(
                    "Stream could not be committed as current state " + currentState + " is not expected state.");
        }
    }

    /**
     * Only records, belonging to committed streams, are marked ready for commit
     *
     * @return true if this stream is committed else false;
     */
    public boolean isReadyForOffsetCommit() {
        return currentState == StreamState.COMMITTED;
    }

    /**
     * Streams which are committed on bigquery table side as well as the connector side are marked inactive
     */
    public void markInactive() {
        currentState = StreamState.INACTIVE;
        this.jsonWriter.close();
    }

    @Override
    public String toString() {
        return "ApplicationStream{" +
                "currentState=" + currentState +
                ", tableName='" + tableName + '\'' +
                ", offsetInformation=" + offsetInformation +
                ", appendCalls=" + appendCalls +
                ", completedCalls=" + completedCalls +
                ", maxCalls=" + maxCalls +
                '}';
    }

    public boolean isInactive() {
        return currentState == StreamState.INACTIVE;
    }

    private void resetStream() {
        if (this.jsonWriter.isClosed()) {
            logger.trace("Recreating stream on table {}", tableName);
            try {
                generateStream();
                logger.trace("Stream recreated successfully on table {}", tableName);
            } catch (Exception exception) {
                throw new BigQueryStorageWriteApiConnectException(
                        String.format(
                                "Stream Writer recreation attempt failed on stream %s due to %s",
                                getStreamName(),
                                exception.getMessage())
                );
            }
        } else {
            logger.trace("Not attempting stream recreation on table {} as Json writer is not closed!", tableName);
        }
    }

    public StreamState getCurrentState() {
        return this.currentState;
    }
}
