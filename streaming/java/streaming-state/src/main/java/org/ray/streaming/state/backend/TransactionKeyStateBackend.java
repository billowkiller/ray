package org.ray.streaming.state.backend;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.ray.streaming.state.ITransactionState;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor.DescType;
import org.ray.streaming.state.keystate.state.facade.ListStateFacade;
import org.ray.streaming.state.keystate.state.facade.MapStateFacade;
import org.ray.streaming.state.keystate.state.facade.ValueStateFacade;
import org.ray.streaming.state.store.IKMapStore;
import org.ray.streaming.state.store.IKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transaction support primitive operations like finish, commit, ackcommit and rollback.
 */
public abstract class TransactionKeyStateBackend implements ITransactionState {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionKeyStateBackend.class);

  protected long currentBatchId;
  protected Object currentKey;
  protected int keyGroupIndex = -1;
  protected Map<String, ValueStateFacade> valueStateMngMap = new HashMap<>();
  protected Map<String, ListStateFacade> listStateMngMap = new HashMap<>();
  protected Map<String, MapStateFacade> mapStateMngMap = new HashMap<>();
  protected Set<String> descNamespace;

  /**
   * tablename, IKVStore key, batchId, content
   */
  protected Map<String, IKVStore<String, Map<Long, byte[]>>> backStorageMap;
  private AbstractStateBackend backend;

  public TransactionKeyStateBackend(AbstractStateBackend backend) {
    this.backStorageMap = new HashMap<>();
    this.backend = backend;
    this.descNamespace = new HashSet<>();
  }

  public <K, T> void put(AbstractStateDescriptor descriptor, K key, T value) {
    String desc = descriptor.getIdentify();
    if (descriptor.getDescType() == DescType.value) {
      if (this.valueStateMngMap.containsKey(desc)) {
        valueStateMngMap.get(desc).put((String) key, value);
      }
    } else if (descriptor.getDescType() == DescType.list) {
      if (this.listStateMngMap.containsKey(desc)) {
        listStateMngMap.get(desc).put((String) key, (List) value);
      }
    } else if (descriptor.getDescType() == DescType.map) {
      if (this.mapStateMngMap.containsKey(desc)) {
        mapStateMngMap.get(desc).put((String) key, (Map) value);
      }
    }
  }

  public <K, T> T get(AbstractStateDescriptor descriptor, K key) {
    String desc = descriptor.getIdentify();
    if (descriptor.getDescType() == DescType.value) {
      if (this.valueStateMngMap.containsKey(desc)) {
        return (T) valueStateMngMap.get(desc).get((String) key);
      }
    } else if (descriptor.getDescType() == DescType.list) {
      if (this.listStateMngMap.containsKey(desc)) {
        return (T) listStateMngMap.get(desc).get((String) key);
      }
    } else if (descriptor.getDescType() == DescType.map) {
      if (this.mapStateMngMap.containsKey(desc)) {
        return (T) mapStateMngMap.get(desc).get((String) key);
      }
    }
    return null;
  }

  @Override
  public void finish(long batchId) {
    for (Entry<String, ValueStateFacade> entry : valueStateMngMap.entrySet()) {
      entry.getValue().finish(batchId);
    }
    for (Entry<String, ListStateFacade> entry : listStateMngMap.entrySet()) {
      entry.getValue().finish(batchId);
    }
    for (Entry<String, MapStateFacade> entry : mapStateMngMap.entrySet()) {
      entry.getValue().finish(batchId);
    }
  }

  @Override
  public Object commit(long batchId, Object state) {

    for (Entry<String, ValueStateFacade> entry : valueStateMngMap.entrySet()) {
      entry.getValue().commit(batchId, entry.getValue().getValueState());
    }
    for (Entry<String, ListStateFacade> entry : listStateMngMap.entrySet()) {
      entry.getValue().commit(batchId, entry.getValue().getListState());
    }
    for (Entry<String, MapStateFacade> entry : mapStateMngMap.entrySet()) {
      entry.getValue().commit(batchId, entry.getValue().getMapState());
    }
    return null;
  }

  @Override
  public void ackCommit(long batchId, long timeStamp) {
    for (Entry<String, ValueStateFacade> entry : valueStateMngMap.entrySet()) {
      entry.getValue().ackCommit(batchId, timeStamp);
    }
    for (Entry<String, ListStateFacade> entry : listStateMngMap.entrySet()) {
      entry.getValue().ackCommit(batchId, timeStamp);
    }
    for (Entry<String, MapStateFacade> entry : mapStateMngMap.entrySet()) {
      entry.getValue().ackCommit(batchId, timeStamp);
    }
  }

  @Override
  public void rollBack(long batchId) {
    for (Entry<String, ValueStateFacade> entry : valueStateMngMap.entrySet()) {
      LOGGER.warn("backend rollback:{},{}", entry.getKey(), batchId);
      entry.getValue().rollBack(batchId);
    }
    for (Entry<String, ListStateFacade> entry : listStateMngMap.entrySet()) {
      LOGGER.warn("backend rollback:{},{}", entry.getKey(), batchId);
      entry.getValue().rollBack(batchId);
    }
    for (Entry<String, MapStateFacade> entry : mapStateMngMap.entrySet()) {
      LOGGER.warn("backend rollback:{},{}", entry.getKey(), batchId);
      entry.getValue().rollBack(batchId);
    }
  }

  public IKVStore<String, Map<Long, byte[]>> getBackStorage(String tableName) {
    if (this.backStorageMap.containsKey(tableName)) {
      return this.backStorageMap.get(tableName);
    } else {
      IKMapStore<String, Long, byte[]> ikvStore = this.backend.getKeyMapStore(tableName);
      this.backStorageMap.put(tableName, ikvStore);
      return ikvStore;
    }
  }

  public IKVStore<String, Map<Long, byte[]>> getBackStorage(
      AbstractStateDescriptor stateDescriptor) {
    String tableName = this.backend.getTableName(stateDescriptor);
    return getBackStorage(tableName);
  }

  public StorageMode getStorageMode() {
    return this.backend.getStorageMode();
  }

  public BackendType getBackendType() {
    return this.backend.getBackendType();
  }

  public Object getCurrentKey() {
    return this.currentKey;
  }

  public abstract void setCurrentKey(Object currentKey);

  public long getBatchId() {
    return this.currentBatchId;
  }

  public void setBatchId(long batchId) {
    this.currentBatchId = batchId;
  }

  public void setContext(long batchId, Object currentKey) {
    setBatchId(batchId);
    setCurrentKey(currentKey);
  }

  public AbstractStateBackend getBackend() {
    return backend;
  }

  public int getKeyGroupIndex() {
    return this.keyGroupIndex;
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }
}
