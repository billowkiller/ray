package org.ray.streaming.state.keystate.desc;

import java.util.Map;
import org.ray.streaming.state.keystate.state.MapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapStateDescriptor.
 *
 * @author wutao on 2019/7/25
 */
public class MapStateDescriptor<K, V> extends AbstractStateDescriptor<MapState<K, V>, Map<K, V>> {

  private static Logger LOGGER = LoggerFactory.getLogger(MapStateDescriptor.class);

  public MapStateDescriptor(String name, Class<K> keyType, Class<V> valueType) {
    super(name, null);
    LOGGER.debug("keyType {}, valueType {}", keyType, valueType);
  }

  public static <K, V> MapStateDescriptor<K, V> build(String name, Class<K> keyType,
                                                                  Class<V> valueType) {
    return new MapStateDescriptor<>(name, keyType, valueType);
  }

  @Override
  public DescType getDescType() {
    return DescType.map;
  }
}
