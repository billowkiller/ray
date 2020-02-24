package org.ray.streaming.state;

/**
 * Key Value State interface.
 *
 * @author wutao on 2018/8/3.
 */
public interface IKVState<K, V> {

  V get(K key);

  void put(K k, V v);
}
