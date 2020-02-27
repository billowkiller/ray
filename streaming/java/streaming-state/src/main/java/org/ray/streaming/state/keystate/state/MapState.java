package org.ray.streaming.state.keystate.state;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * MapState interface.
 */
public interface MapState<K, V> extends State {

  Map<K, V> get();

  /**
   * Returns the current value associated with the given key.
   *
   * @param key The key of the mapping
   * @return The value of the mapping with the given key
   */
  V get(K key);

  /**
   * Associates a new value with the given key.
   *
   * @param key The key of the mapping
   * @param value The new value of the mapping
   */
  void put(K key, V value);

  /**
   * Resets the state value.
   *
   * @param map The mappings for reset in this state
   */
  void update(Map<K, V> map);

  /**
   * Copies all of the mappings from the given map into the state.
   *
   * @param map The mappings to be stored in this state
   */
  void putAll(Map<K, V> map);

  /**
   * Deletes the mapping of the given key.
   *
   * @param key The key of the mapping
   */
  void remove(K key);

  /**
   * Returns whether there exists the given mapping.
   *
   * @param key The key of the mapping
   * @return True if there exists a mapping whose key equals to the given key
   */
  boolean contains(K key);

  /**
   * Returns all the mappings in the state
   *
   * @return An iterable view of all the key-value pairs in the state.
   */
  Iterable<Entry<K, V>> entries();

  /**
   * Returns all the keys in the state
   *
   * @return An iterable view of all the keys in the state.
   */
  Iterable<K> keys();

  /**
   * Returns all the values in the state.
   *
   * @return An iterable view of all the values in the state.
   */
  Iterable<V> values();

  /**
   * Iterates over all the mappings in the state.
   *
   * @return An iterator over all the mappings in the state
   */
  Iterator<Entry<K, V>> iterator();
}
