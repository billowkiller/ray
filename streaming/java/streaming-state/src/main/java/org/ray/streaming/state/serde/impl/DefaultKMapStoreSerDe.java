package org.ray.streaming.state.serde.impl;

import org.ray.streaming.state.serde.IKMapStoreSerDe;
import org.ray.streaming.state.serde.SerDeHelper;

/**
 * KEY MAP SER DE.
 */
public class DefaultKMapStoreSerDe<K, S, T> extends AbstractSerDe
    implements IKMapStoreSerDe<K, S, T> {

  @Override
  public byte[] serKey(K key) {
    String keyWithPrefix = generateRowKeyPrefix(key.toString());
    return keyWithPrefix.getBytes();
  }

  @Override
  public byte[] serUKey(S uk) {
    return SerDeHelper.object2Byte(uk);
  }

  @Override
  public S deSerUKey(byte[] ukArray) {
    return (S) SerDeHelper.byte2Object(ukArray);
  }

  @Override
  public byte[] serUValue(T uv) {
    return SerDeHelper.object2Byte(uv);
  }

  @Override
  public T deSerUValue(byte[] uvArray) {
    return (T) SerDeHelper.byte2Object(uvArray);
  }
}
