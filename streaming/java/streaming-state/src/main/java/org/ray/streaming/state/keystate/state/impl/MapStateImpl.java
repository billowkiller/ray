package org.ray.streaming.state.keystate.state.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.state.MapState;

/**
 * MapState implementation.
 */
public class MapStateImpl<K, V> extends AbstractState<Map<K, V>> implements MapState<K, V> {

  public MapStateImpl(MapStateDescriptor<K, V> descriptor, KeyStateBackend backend) {
    super(backend, descriptor);
  }

  @Override
  public Map<K, V> get() {
    Map<K, V> map = this.get(descriptor);
    if (map == null) {
      map = new HashMap<>();
    }
    return map;
  }

  @Override
  public V get(K key) {
    Map<K, V> map = get();
    return map.get(key);
  }

  @Override
  public void put(K key, V value) {
    Map<K, V> map = get();

    map.put(key, value);
    this.put(descriptor, map);
  }

  @Override
  public void update(Map<K, V> map) {
    if (map == null) {
      map = new HashMap<>();
    }
    this.put(descriptor, map);
  }

  @Override
  public void putAll(Map<K, V> newMap) {
    Map<K, V> map = get();

    map.putAll(newMap);
    this.put(descriptor, map);
  }

  @Override
  public void remove(K key) {
    Map<K, V> map = get();

    map.remove(key);
    this.put(descriptor, map);
  }

  @Override
  public boolean contains(K key) {
    return get().containsKey(key);
  }

  @Override
  public Iterable<Entry<K, V>> entries() {
    return get().entrySet();
  }

  @Override
  public Iterable<K> keys() {
    return get().keySet();
  }

  @Override
  public Iterable<V> values() {
    return get().values();
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    return get().entrySet().iterator();
  }
}
