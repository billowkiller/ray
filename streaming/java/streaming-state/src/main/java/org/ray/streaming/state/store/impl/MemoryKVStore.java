package org.ray.streaming.state.store.impl;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.ray.streaming.state.store.IKVStore;

/**
 * Memory Key Value Store.
 */
public class MemoryKVStore<K, V> implements IKVStore<K, V> {

  private Map<K, V> memoryStore;

  public MemoryKVStore() {
    this.memoryStore = Maps.newConcurrentMap();
  }

  @Override
  public void put(K key, V value) throws IOException {
    this.memoryStore.put(key, value);
  }

  @Override
  public V get(K key) throws IOException {
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
