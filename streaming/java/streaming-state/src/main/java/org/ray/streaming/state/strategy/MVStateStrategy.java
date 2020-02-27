package org.ray.streaming.state.strategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.ray.streaming.state.StateException;
import org.ray.streaming.state.StorageRecord;
import org.ray.streaming.state.keystate.state.impl.AbstractState;
import org.ray.streaming.state.store.IKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class define the multi-version store strategy, which leverages external storage's mvcc.
 */
public class MVStateStrategy<V> extends AbstractStateStrategy<V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MVStateStrategy.class);

  public MVStateStrategy(IKVStore<String, Map<Long, byte[]>> kvStore) {
    super(kvStore);
  }

  @Override
  public void finish(long batchId) {
    Map<String, byte[]> currentStateRecords = new HashMap<>();
    for (Entry<String, StorageRecord<V>> entry : frontStore.entrySet()) {
      currentStateRecords.put(entry.getKey(), toByte(entry.getValue()));
    }

    middleStore.put(batchId, currentStateRecords);
    frontStore.clear();
  }

  @Override
  public Object commit(long batchId, Object state) {
    // write to external storage
    List<Long> batchIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(batchIds);

    for (int i = batchIds.size() - 1; i >= 0; i--) {
      long commitBatchId = batchIds.get(i);
      if (commitBatchId > batchId) {
        continue;
      }

      Map<String, byte[]> commitRecords = middleStore.get(commitBatchId);

      try {
        for (Entry<String, byte[]> entry : commitRecords.entrySet()) {

          Map<Long, byte[]> remoteData = this.kvStore.get(entry.getKey());
          if (remoteData == null) {
            remoteData = new HashMap<>();
          }

          remoteData.put(commitBatchId, entry.getValue());

          this.kvStore.put(entry.getKey(), remoteData);
        }
        this.kvStore.flush();
      } catch (Exception e) {
        throw new StateException(e);
      }
    }

    return null;
  }

  @Override
  public void rollBack(long batchId) {
    this.frontStore.clear();
    this.middleStore.clear();
    this.kvStore.clearCache();
  }

  @Override
  public V get(long batchId, String key, AbstractState state) {
    StorageRecord<V> valueArray = frontStore.get(key);
    if (valueArray != null) {
      return valueArray.getValue();
    } else {
      List<Long> batchIds = new ArrayList<>(middleStore.keySet());
      Collections.sort(batchIds);

      for (int i = batchIds.size() - 1; i >= 0; i--) {
        if (batchIds.get(i) > batchId) {
          continue;
        }

        Map<String, byte[]> records = middleStore.get(batchIds.get(i));
        if (records != null) {
          if (records.containsKey(key)) {
            byte[] bytes = records.get(key);
            return toStorageRecord(bytes).getValue();
          }
        }
      }

      // get from external storage
      try {
        Map<Long, byte[]> remoteData = this.kvStore.get(key);
        if (remoteData != null) {
          batchIds = new ArrayList<>(remoteData.keySet());
          Collections.sort(batchIds);

          for (int i = batchIds.size() - 1; i >= 0; i--) {
            if (batchIds.get(i) > batchId) {
              continue;
            }

            byte[] bytes = remoteData.get(batchIds.get(i));
            return toStorageRecord(bytes).getValue();
          }
        }
      } catch (Exception e) {
        throw new StateException(e);
      }
    }
    return null;
  }

  @Override
  public void put(long batchId, String k, V v) {
    frontStore.put(k, new StorageRecord<>(batchId, v));
  }

  @Override
  public void ackCommit(long batchId) {
    List<Long> batchIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(batchIds);

    for (int i = batchIds.size() - 1; i >= 0; i--) {
      long commitBatchId = batchIds.get(i);
      if (commitBatchId > batchId) {
        continue;
      }

      this.middleStore.remove(commitBatchId);
    }
  }

}
