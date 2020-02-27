package org.ray.streaming.state.serde;

/**
 * Key Value serde.
 */
public interface IKVStoreSerDe<K, V> {

  byte[] serKey(K key);

  byte[] serValue(V value);

  V deSerValue(byte[] valueArray);
}
