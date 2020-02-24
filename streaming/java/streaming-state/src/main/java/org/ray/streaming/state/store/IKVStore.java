package org.ray.streaming.state.store;

import java.io.IOException;

/**
 * Key Value Store interface.
 * Created by eagle on 2018/8/6.
 */
public interface IKVStore<K, V> extends IStore {

  void put(K key, V value) throws IOException;

  V get(K key) throws IOException;

  void remove(K key) throws IOException;

  void flush() throws IOException;

  void clearCache();

  void close() throws IOException;
}
