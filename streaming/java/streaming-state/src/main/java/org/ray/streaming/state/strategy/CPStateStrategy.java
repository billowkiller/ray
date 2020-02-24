package org.ray.streaming.state.strategy;

import com.google.common.primitives.Longs;
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
 * This class define the checkpoint store strategy, which saves two-version data once.
 * Created by eagle on 2019/7/31.
 */
public class CPStateStrategy<V> extends AbstractStateStrategy<V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CPStateStrategy.class);

  private static final int cpBatchSize = 5;

  public CPStateStrategy(IKVStore<String, Map<Long, byte[]>> backStore) {
    super(backStore);
  }

  @Override
  public void finish(long batchId) {
    if (batchId % cpBatchSize == 0) {
      LOGGER.info("do finish batchId:{}", batchId);
      Map<String, byte[]> cpStore = new HashMap<>();
      for (Entry<String, StorageRecord<V>> entry : frontStore.entrySet()) {
        String key = entry.getKey();
        StorageRecord<V> value = entry.getValue();
        cpStore.put(key, toByte(value));
      }
      middleStore.put(batchId, cpStore);
      frontStore.clear();
    }
  }

  @Override
  public Object commit(long batchId, Object state) {
    if (batchId % cpBatchSize == 0) {
      try {
        LOGGER.info("do commit batchId:{}", batchId);
        Map<String, byte[]> cpStore = middleStore.get(batchId);
        if (cpStore == null) {
          throw new StateException("why cp store is null");
        }
        for (Entry<String, byte[]> entry : cpStore.entrySet()) {
          String key = entry.getKey();
          byte[] value = entry.getValue();

          //2 is new，1 is old，-1 indicates old batchId, -2 indicates new batchId
          Map<Long, byte[]> remoteData = super.kvStore.get(key);
          if (remoteData == null || remoteData.size() == 0) {
            remoteData = new HashMap<>();
            remoteData.put(2L, value);
            remoteData.put(-2L, Longs.toByteArray(batchId));
          } else {
            long oldBatchId = Longs.fromByteArray(remoteData.get(-2L));
            if (oldBatchId < batchId) {
              //move the old data
              remoteData.put(1L, remoteData.get(2L));
              remoteData.put(-1L, remoteData.get(-2L));
            }

            //put the new data here
            remoteData.put(2L, value);
            remoteData.put(-2L, Longs.toByteArray(batchId));
          }
          super.kvStore.put(key, remoteData);
        }
        super.kvStore.flush();
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        throw new StateException(e);
      }
    }
    return null;
  }

  @Override
  public void rollBack(long batchId) {
    LOGGER.info("do rollBack batchId:{}", batchId);
    this.frontStore.clear();
    this.middleStore.clear();
    this.kvStore.clearCache();
  }

  @Override
  public V get(long batchId, String key, AbstractState state) {
    // get from current cp cache
    StorageRecord<V> storageRecord = frontStore.get(key);
    if (storageRecord != null) {
      return storageRecord.getValue();
    }

    //get from not commit cp info
    List<Long> batchIds = new ArrayList<>(middleStore.keySet());
    Collections.sort(batchIds);
    for (int i = batchIds.size() - 1; i >= 0; i--) {
      Map<String, byte[]> cpStore = middleStore.get(batchIds.get(i));
      if (cpStore != null) {
        if (cpStore.containsKey(key)) {
          byte[] cpData = cpStore.get(key);
          storageRecord = toStorageRecord(cpData);
          return storageRecord.getValue();
        }
      }
    }

    try {
      Map<Long, byte[]> remoteData = super.kvStore.get(key);
      if (remoteData != null) {
        for (Entry<Long, byte[]> entry : remoteData.entrySet()) {
          if (entry.getKey() > 0) { // 小于0的是存储的batchId
            StorageRecord<V> tmp = toStorageRecord(entry.getValue());
            if (tmp.getBatchId() < batchId) {
              if (storageRecord == null) {
                storageRecord = tmp;
              } else if (storageRecord.getBatchId() < tmp.getBatchId()) {
                storageRecord = tmp;
              }
            }
          }
        }
        if (storageRecord != null) {
          return storageRecord.getValue();
        }
      }
    } catch (Exception e) {
      LOGGER.error("get batchId:" + batchId + " key:" + key, e);
      throw new StateException(e);
    }
    return null;
  }

  @Override
  public void ackCommit(long batchId) {
    if (batchId % cpBatchSize == 0) {
      LOGGER.info("do ackCommit batchId:{}", batchId);
      middleStore.remove(batchId);
    }
  }
}
