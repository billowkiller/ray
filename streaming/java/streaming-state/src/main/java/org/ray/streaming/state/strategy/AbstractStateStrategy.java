package org.ray.streaming.state.strategy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.streaming.state.ITransactionState;
import org.ray.streaming.state.StateException;
import org.ray.streaming.state.StorageRecord;
import org.ray.streaming.state.keystate.state.impl.AbstractState;
import org.ray.streaming.state.serde.SerDeHelper;
import org.ray.streaming.state.store.IKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines the StoreManager Abstract class.
 * We use three layer to store the state, frontStore, middleStore and kvStore(remote).
 */
public abstract class AbstractStateStrategy<V> implements ITransactionState {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStateStrategy.class);

  /**
   * read-write
   */
  protected Map<String, StorageRecord<V>> frontStore = new ConcurrentHashMap<>();

  /**
   * remote-storage
   */
  protected IKVStore<String, Map<Long, byte[]>> kvStore;

  /**
   * read-only
   */
  protected Map<Long, Map<String, byte[]>> middleStore = new ConcurrentHashMap<>();
  protected int keyGroupIndex = -1;

  public AbstractStateStrategy(IKVStore<String, Map<Long, byte[]>> backStore) {
    kvStore = backStore;
  }

  public byte[] toByte(StorageRecord storageRecord) {
    return SerDeHelper.object2Byte(storageRecord);
  }

  public StorageRecord<V> toStorageRecord(byte[] data) {
    return (StorageRecord<V>) SerDeHelper.byte2Object(data);
  }

  public abstract V get(long batchId, String key, AbstractState state);

  public void put(long batchId, String k, V v) {
    frontStore.put(k, new StorageRecord<>(batchId, v));
  }

  @Override
  public void ackCommit(long batchId, long timeStamp) {
    ackCommit(batchId);
  }

  public abstract void ackCommit(long batchId);

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }

  public void close() {
    frontStore.clear();
    middleStore.clear();
    if (kvStore != null) {
      kvStore.clearCache();
      try {
        kvStore.close();
      } catch (IOException e) {
        throw new StateException(e);
      }
    }
  }
}
