package com.wepay.kafka.connect.bigquery.write;

import java.util.List;

public class RecordBatches<E> {

  private final List<E> records;

  private int batchStart;
  private int batchSize;

  public RecordBatches(List<E> records) {
    this.records = records;
    this.batchStart = 0;
    this.batchSize = records.size();
  }

  public List<E> currentBatch() {
    int size = Math.max(records.size() - batchStart, batchSize);
    return records.subList(batchStart, size);
  }

  public void advanceToNextBatch() {
    batchStart += batchSize;
  }

  public void reduceBatchSize() {
    if (!canReduceBatchSize()) {
      throw new IllegalStateException("Cannot reduce batch size any further");
    }
    batchSize /= 2;
  }

  public boolean completed() {
    return batchStart >= records.size();
  }

  public boolean canReduceBatchSize() {
    return batchSize <= 1;
  }

}
