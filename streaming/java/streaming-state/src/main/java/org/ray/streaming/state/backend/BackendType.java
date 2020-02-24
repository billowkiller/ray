package org.ray.streaming.state.backend;

/**
 * @author wutao on 2018/8/10.
 */
public enum BackendType {
  /**
   * MEMORY
   */
  MEMORY;

  public static BackendType getEnum(String value) {
    for (BackendType v : values()) {
      if (v.name().equalsIgnoreCase(value)) {
        return v;
      }
    }
    return MEMORY;
  }
}
