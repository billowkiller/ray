package org.ray.streaming.state.serde;

import org.nustaq.serialization.FSTConfiguration;

/**
 * fst wrapper.
 */
public class SerDeHelper {

  private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(() -> {
    FSTConfiguration configuration = FSTConfiguration.createDefaultConfiguration();
    return configuration;
  });

  public static byte[] object2Byte(Object value) {
    return conf.get().asByteArray(value);
  }

  public static Object byte2Object(byte[] buffer) {
    return conf.get().asObject(buffer);
  }
}
