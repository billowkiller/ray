package org.ray.streaming.state;

/**
 * TransactionState interface.
 */
public interface ITransactionState {

  void finish(long batchId);

  Object commit(long batchId, Object state);

  void ackCommit(long batchId, long timeStamp);

  void rollBack(long batchId);
}
