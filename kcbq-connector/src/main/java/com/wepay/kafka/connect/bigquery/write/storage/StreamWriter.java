package com.wepay.kafka.connect.bigquery.write.storage;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.protobuf.Descriptors;
import java.io.IOException;
import org.json.JSONArray;

public interface StreamWriter extends AutoCloseable {

  /**
   * Write the provided rows
   *
   * @param rows the rows to write; may not be null
   * @return the response from BigQuery for the write attempt
   */
  ApiFuture<AppendRowsResponse> appendRows(
      JSONArray rows
  ) throws Descriptors.DescriptorValidationException, IOException;

  /**
   * Invoked if the underlying stream appears to be closed. Implementing classes
   * should respond by re-initialize the underlying stream.
   */
  void refresh();

  /**
   * Invoked when all rows have either been written to BigQuery or intentionally
   * discarded (e.g., reported to an {@link com.wepay.kafka.connect.bigquery.ErrantRecordHandler}).
   */
  void onSuccess();

  /**
   * Inherited from {@link AutoCloseable}, but overridden to remove the throws clause
   */
  void close();

}
