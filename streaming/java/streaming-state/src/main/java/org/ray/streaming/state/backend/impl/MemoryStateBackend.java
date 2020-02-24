package org.ray.streaming.state.backend.impl;

import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.serde.IKMapStoreSerDe;
import org.ray.streaming.state.store.IKMapStore;
import org.ray.streaming.state.store.IKVStore;
import org.ray.streaming.state.store.impl.MemoryKMapStore;
import org.ray.streaming.state.store.impl.MemoryKVStore;

/**
 * @author wutao on 2019/7/26
 */
public class MemoryStateBackend extends AbstractStateBackend {

  public MemoryStateBackend(Map<String, String> config) {
    super(config);
  }

  @Override
  public <K, V> IKVStore<K, V> getKeyValueStore(String tableName) {
    return new MemoryKVStore<>();
  }

  @Override
  public <K, S, T> IKMapStore<K, S, T> getKeyMapStore(String tableName) {
    return new MemoryKMapStore<>();
  }

  @Override
  public <K, S, T> IKMapStore<K, S, T> getKeyMapStore(String tableName,
                                                          IKMapStoreSerDe ikMapStoreSerDe) {
    return new MemoryKMapStore<>();
  }

}
