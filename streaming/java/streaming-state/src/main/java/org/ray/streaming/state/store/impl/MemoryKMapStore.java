package org.ray.streaming.state.store.impl;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.store.IKMapStore;

/**
 * Memory Key Map Store.
 * Created by eagle on 2018/8/6.
 */
public class MemoryKMapStore<K, S, T> implements IKMapStore<K, S, T> {

  private Map<K, Map<S, T>> memoryStore;

  public MemoryKMapStore() {
    this.memoryStore = Maps.newConcurrentMap();
  }

  @Override
  public void put(K key, S subKey, T value) throws IOException {
    if (memoryStore.containsKey(key)) {
      memoryStore.get(key).put(subKey, value);
    } else {
      Map<S, T> map = new HashMap<>();
      map.put(subKey, value);
      memoryStore.put(key, map);
    }
  }

  @Override
  public void put(K key, Map<S, T> value) throws IOException {
    this.memoryStore.put(key, value);
  }

  @Override
  public T get(K key, S subKey) throws IOException {
    if (memoryStore.containsKey(key)) {
      return memoryStore.get(key).get(subKey);
    }
    return null;
  }


  @Override
  public Map<S, T> get(K key) throws IOException {
    return this.memoryStore.get(key);
  }

  @Override
  public void remove(K key) throws IOException {
    this.memoryStore.remove(key);
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void clearCache() {

  }

  @Override
  public void close() throws IOException {
    if (memoryStore != null) {
      memoryStore.clear();
    }
  }

}
