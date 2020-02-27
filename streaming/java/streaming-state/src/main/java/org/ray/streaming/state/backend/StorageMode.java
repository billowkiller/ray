package org.ray.streaming.state.backend;

/**
 * Storage model.
 */
public enum StorageMode {
  /**
   * save two version together in case of rollback.
   */
  DUALVERSION,

  /**
   * for storage supporting mvcc, we save only current version.
   */
  SINGLEVERSION,
  /**
   * other storage mode.
   */
  OTHER;

  public static StorageMode getEnum(String value) {
    for (StorageMode v : values()) {
      if (v.name().equalsIgnoreCase(value)) {
        return v;
      }
    }
    return OTHER;
  }
}
