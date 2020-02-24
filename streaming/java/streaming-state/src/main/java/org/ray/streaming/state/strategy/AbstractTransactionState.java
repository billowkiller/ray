package org.ray.streaming.state.strategy;

import java.util.Map;
import org.ray.streaming.state.ITransactionState;
import org.ray.streaming.state.backend.StorageMode;
import org.ray.streaming.state.store.IKVStore;


/**
 * This class support ITransactionState.
 * Created by eagle on 2019/7/30.
 */

public abstract class AbstractTransactionState<V> implements ITransactionState {

  protected AbstractStateStrategy<V> stateStrategy;

  public AbstractTransactionState(IKVStore<String, Map<Long, byte[]>> backStorage,
                                  StorageMode storageMode) {
    switch (storageMode) {
      case DUALVERSION:
        this.stateStrategy = new CPStateStrategy<>(backStorage);
        break;
      case SINGLEVERSION:
        this.stateStrategy = new MVStateStrategy<>(backStorage);
        break;
      default:
        throw new UnsupportedOperationException("store vertexType not support");
    }
  }

  @Override
  public void finish(long batchId) {
    this.stateStrategy.finish(batchId);
  }

  @Override
  public Object commit(long batchId, Object state) {
    this.stateStrategy.commit(batchId, state);
    return null;
  }

  @Override
  public void ackCommit(long batchId, long timeStamp) {
    this.stateStrategy.ackCommit(batchId);
  }

  @Override
  public void rollBack(long batchId) {
    this.stateStrategy.rollBack(batchId);
  }

  public void close() {
    this.stateStrategy.close();
  }
}
