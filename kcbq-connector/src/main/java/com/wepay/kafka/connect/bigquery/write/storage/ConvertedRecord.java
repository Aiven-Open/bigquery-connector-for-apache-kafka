package com.wepay.kafka.connect.bigquery.write.storage;

import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONObject;

public class ConvertedRecord {

  private final SinkRecord original;
  private final JSONObject converted;

  public ConvertedRecord(SinkRecord original, JSONObject converted) {
    this.original = original;
    this.converted = converted;
  }

  public SinkRecord original() {
    return original;
  }

  public JSONObject converted() {
    return converted;
  }

}
