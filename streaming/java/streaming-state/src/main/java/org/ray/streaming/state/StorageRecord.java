package org.ray.streaming.state;

import java.io.Serializable;

/**
 * save record with batchId.
 * Created by eagle on 2019/7/30.
 */
public class StorageRecord<T> implements Serializable {

  private long batchId;
  private T value;

  public StorageRecord() {
  }

  public StorageRecord(long batchId, T value) {
    this.batchId = batchId;
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  @Override
  public String toString() {
    if (value != null) {
      return "batchId:" + batchId + ", value:" + value;
    } else {
      return "batchId:" + batchId + ", value:null";
    }
  }
}
