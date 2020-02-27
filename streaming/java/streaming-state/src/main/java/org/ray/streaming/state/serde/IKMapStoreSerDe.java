package org.ray.streaming.state.serde;

/**
 * Key Map serde.
 */
public interface IKMapStoreSerDe<K, S, T> {

  byte[] serKey(K key);

  byte[] serUKey(S uk);

  S deSerUKey(byte[] ukArray);

  byte[] serUValue(T uv);

  T deSerUValue(byte[] uvArray);

}
