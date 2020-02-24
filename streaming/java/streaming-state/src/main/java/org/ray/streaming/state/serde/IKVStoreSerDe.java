package org.ray.streaming.state.serde;

/**
 * Key Value serde.
 * Created by eagle on 2018/8/28.
 */
public interface IKVStoreSerDe<K, V> {

  byte[] serKey(K key);

  byte[] serValue(V value);

  V deSerValue(byte[] valueArray);
}
