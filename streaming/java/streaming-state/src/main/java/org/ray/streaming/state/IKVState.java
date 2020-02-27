package org.ray.streaming.state;

/**
 * Key Value State interface.
 */
public interface IKVState<K, V> {

  V get(K key);

  void put(K k, V v);
}
