package org.ray.streaming.state.store;

import java.io.IOException;
import java.util.Map;

/**
 * Key Map Store interface.
 * Created by eagle on 2018/8/6.
 */
public interface IKMapStore<K, S, T> extends IKVStore<K, Map<S, T>> {

  void put(K key, S subKey, T value) throws IOException;

  T get(K key, S subKey) throws IOException;
}
