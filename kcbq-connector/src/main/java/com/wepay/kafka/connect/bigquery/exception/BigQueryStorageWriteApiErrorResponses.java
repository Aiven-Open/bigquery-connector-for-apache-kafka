/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.exception;

import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.rpc.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util for storage Write API error responses. This new API uses gRPC protocol.
 * gRPC code : https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.rpc#google.rpc.Code
 */
public class BigQueryStorageWriteApiErrorResponses {

  private static final Logger logger = LoggerFactory.getLogger(BigQueryStorageWriteApiErrorResponses.class);
  private static final String NOT_EXIST = "(or it may not exist)";
  private static final String NOT_FOUND = "Not found: table";
  private static final String TABLE_IS_DELETED = "Table is deleted";
  private static final String MESSAGE_TOO_LARGE = "MessageSize is too large";
  private static final String APPEND_ROWS_REQUEST_TOO_LARGE = "AppendRows request too large";
  private static final String[] retriableCodes = {Code.INTERNAL.name(), Code.ABORTED.name(), Code.CANCELLED.name()};
  /*
   Below list is taken from :
   https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#storageerrorcode
   */
  private static final Set<String> nonRetriableStreamFailureCodes = new HashSet<>(Arrays.asList(
      StorageError.StorageErrorCode.STREAM_FINALIZED.name(),
      StorageError.StorageErrorCode.STREAM_NOT_FOUND.name(),
      StorageError.StorageErrorCode.INVALID_STREAM_STATE.name(),
      StorageError.StorageErrorCode.INVALID_STREAM_TYPE.name(),
      StorageError.StorageErrorCode.STORAGE_ERROR_CODE_UNSPECIFIED.name(),
      StorageError.StorageErrorCode.STREAM_ALREADY_COMMITTED.name()
  ));
  private static final String MORE_FIELDS_THAN_BIGQUERY_SCHEMA = "Input schema has more fields than BigQuery schema";
  private static final String UNKNOWN_FIELD = "The source object has fields unknown to BigQuery";
  private static final String MISSING_REQUIRED_FIELD = "JSONObject does not have the required field";
  private static final String STREAM_CLOSED = "StreamWriterClosedException";


  /**
   * Expected BigQuery Table does not exist
   *
   * @param errorMessage Message from the received exception
   * @return Returns true if message contains table missing substrings
   */
  public static boolean isTableMissing(String errorMessage) {
    return (errorMessage.contains(Code.PERMISSION_DENIED.name()) && errorMessage.contains(NOT_EXIST))
        || (errorMessage.contains(StorageError.StorageErrorCode.TABLE_NOT_FOUND.name()))
        || errorMessage.contains(NOT_FOUND)
        || errorMessage.contains(Code.NOT_FOUND.name())
        || errorMessage.contains(TABLE_IS_DELETED);
  }

  /**
   * The list of retriable code is taken write api sample codes and gRpc code page
   *
   * @param errorMessage Message from the received exception
   * @return Retruns true if the exception is retriable
   */
  public static boolean isRetriableError(String errorMessage) {
    return Arrays.stream(retriableCodes).anyMatch(errorMessage::contains);
  }

  /**
   * Indicates user input is incorrect
   *
   * @param errorMessage Exception message received on append call
   * @return Returns if the exception is due to bad input
   */
  public static boolean isMalformedRequest(String errorMessage) {
    return errorMessage.contains(Code.INVALID_ARGUMENT.name());
  }

  /**
   * Tells if the exception is caused by an invalid schema in request
   *
   * @param messages List of Row error messages
   * @return Returns true if any of the messages matches invalid schema substrings
   */
  public static boolean hasInvalidSchema(Collection<String> messages) {
    return messages.stream().anyMatch(message ->
        message.contains(UNKNOWN_FIELD)
            || message.contains(MISSING_REQUIRED_FIELD)
            || message.contains(MORE_FIELDS_THAN_BIGQUERY_SCHEMA)
            || message.contains(StorageError.StorageErrorCode.SCHEMA_MISMATCH_EXTRA_FIELDS.name()));
  }

  /**
   * Tells if the exception is caused by auto-close of JSON stream
   *
   * @param errorMessage Exception message received on append call
   * @return Returns true is message contains StreamClosed exception
   */
  public static boolean isStreamClosed(String errorMessage) {
    return errorMessage.contains(STREAM_CLOSED);
  }

  /**
   * Tells if the exception is of storage exception type and  error code belong to the list of non retriable
   * storage error code.
   *
   * @param exception Exception received from Batch mode data ingestion
   * @return Retruns true if the exception is non-retriable
   */
  public static boolean isNonRetriableStorageError(Exception exception) {
    Exceptions.StorageException storageException = null;
    Throwable t = exception.getCause();
    if (t instanceof StatusRuntimeException || t instanceof StatusException) {
      storageException = Exceptions.toStorageException(exception);
    }
    if (storageException == null) {
      // it is not a storage exception. We will consider it something unknown and thus non-retriable
      return true;
    }
    String errorCode = storageException.getStatus().getCode().name();

    logger.trace("Storage exception occurred with errorCode {} and errors {} ", errorCode, storageException.getErrors().toString());

    return nonRetriableStreamFailureCodes.contains(errorCode);
  }

  public static boolean isMessageTooLargeError(String errorMessage) {
    return isMalformedRequest(errorMessage)
        && (errorMessage.contains(MESSAGE_TOO_LARGE)
        || errorMessage.contains(APPEND_ROWS_REQUEST_TOO_LARGE)
      );
  }

}
