package org.ray.streaming.state.serde.impl;

import org.ray.streaming.state.serde.IKVStoreSerDe;
import org.ray.streaming.state.serde.SerDeHelper;

/**
 * KV Store SerDe.
 */
public class DefaultKVStoreSerDe<K, V> extends AbstractSerDe implements IKVStoreSerDe<K, V> {

  @Override
  public byte[] serKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serValue(V value) {
    return SerDeHelper.object2Byte(value);
  }

  @Override
  public V deSerValue(byte[] valueArray) {
    return (V) SerDeHelper.byte2Object(valueArray);
  }
}
