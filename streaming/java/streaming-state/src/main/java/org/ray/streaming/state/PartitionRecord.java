package org.ray.streaming.state;

import java.io.Serializable;

/**
 * value record for partition.
 *
 * @author wutao on 2018/8/4.
 */
public class PartitionRecord<T> implements Serializable {

  private int partitionNum;
  private T value;

  public PartitionRecord() {
  }

  public PartitionRecord(int partitionNum, T value) {
    this.partitionNum = partitionNum;
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }
}
