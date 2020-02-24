package org.ray.streaming.state.keystate.state.facade;

import java.util.Map;
import org.ray.streaming.state.IKVState;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.impl.MapStateImpl;
import org.ray.streaming.state.strategy.AbstractTransactionState;

/**
 * This class defines MapState Wrapper, connecting state and backend.
 *
 * @author wutao on 2018/8/2.
 */
public class MapStateFacade<K, V> extends AbstractTransactionState<Map<K, V>> implements
    IKVState<String, Map<K, V>> {

  private KeyStateBackend keyStateBackend;
  private MapStateImpl<K, V> mapState;

  public MapStateFacade(KeyStateBackend keyStateBackend, MapStateDescriptor<K, V> stateDescriptor) {
    super(keyStateBackend.getBackStorage(stateDescriptor), keyStateBackend.getStorageMode());
    this.keyStateBackend = keyStateBackend;
    this.mapState = new MapStateImpl<>(stateDescriptor, keyStateBackend);
  }

  public MapState<K, V> getMapState() {
    return this.mapState;
  }

  @Override
  public Map<K, V> get(String key) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    return this.stateStrategy.get(this.keyStateBackend.getBatchId(), key, mapState);
  }

  @Override
  public void put(String key, Map<K, V> ts) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    this.stateStrategy.put(this.keyStateBackend.getBatchId(), key, ts);
  }
}
