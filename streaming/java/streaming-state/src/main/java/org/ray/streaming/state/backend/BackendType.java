package org.ray.streaming.state.backend;

/**
 * Backend Types.
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
