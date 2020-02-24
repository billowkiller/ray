package org.ray.streaming.state.backend;

import java.io.Serializable;
import java.util.Map;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.config.ConfigKey;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import org.ray.streaming.state.serde.IKMapStoreSerDe;
import org.ray.streaming.state.store.IKMapStore;
import org.ray.streaming.state.store.IKVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wutao on 2019/7/26
 */
public abstract class AbstractStateBackend implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStateBackend.class);

  protected Map<String, String> config;
  protected StorageMode storageMode;

  protected BackendType backendType;
  protected int keyGroupIndex = -1;

  protected AbstractStateBackend(Map<String, String> config) {
    this.storageMode = StorageMode.getEnum(ConfigKey.getStorageMode(config));
    this.backendType = BackendType.getEnum(ConfigKey.getBackendType(config));
    this.config = config;
  }

  private static AbstractStateBackend getAbstractStateBackend(Map<String, String> config,
                                                              BackendType type) {
    switch (type) {
      case MEMORY:
        return new MemoryStateBackend(config);
      default:
        return new MemoryStateBackend(config);
    }
  }

  public static AbstractStateBackend buildStateBackend(Map<String, String> config) {
    BackendType type;
    if (config == null) {
      type = BackendType.MEMORY;
    } else {
      type = BackendType.getEnum(config.get(ConfigKey.STATE_BACKEND_TYPE));
    }

    return getAbstractStateBackend(config, type);
  }

  public KeyStateBackend createKeyedStateBackend(String operatorIdentifier,
                                                 int numberOfKeyGroups, KeyGroup keyGroup) {
    return new KeyStateBackend(numberOfKeyGroups, keyGroup, this);
  }

  public OperatorStateBackend createOperatorStateBackend(String operatorIdentifier,
                                                         String tableName) {
    return new OperatorStateBackend(this);
  }

  public abstract <K, V> IKVStore<K, V> getKeyValueStore(String tableName);

  public abstract <K, S, T> IKMapStore<K, S, T> getKeyMapStore(String tableName);

  public abstract <K, S, T> IKMapStore<K, S, T> getKeyMapStore(String tableName,
                                                                   IKMapStoreSerDe ikMapStoreSerDe);

  public BackendType getBackendType() {
    return backendType;
  }

  public StorageMode getStorageMode() {
    return storageMode;
  }

  public String getTableName(AbstractStateDescriptor stateDescriptor) {
    return stateDescriptor.getTableName();
  }

  public String getStateKey(String descName, String currentKey) {
    return descName + "_" + currentKey;
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    this.keyGroupIndex = keyGroupIndex;
  }
}
