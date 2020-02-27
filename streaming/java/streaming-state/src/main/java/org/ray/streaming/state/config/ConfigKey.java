package org.ray.streaming.state.config;

import java.util.Map;

/**
 * state config keys.
 */
public class ConfigKey {

  /**
   * backend
   */
  public static final String STATE_BACKEND_TYPE = "state.backend.type";
  public static final String STATE_TABLE_NAME = "state.table.name";
  public static final String STATE_STORAGE_MODE = "storage.mode";
  public static final String NUMBER_PER_CHECKPOINT = "number.per.checkpoint";
  public static final String JOB_MAX_PARALLEL = "job.max.parallel";

  public static final String getStorageMode(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_STORAGE_MODE, "DUALVERSION");
  }

  public static final String getBackendType(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_BACKEND_TYPE, "MEMORY");
  }

  public static int getNumberPerCheckpoint(Map<String, String> config) {
    return ConfigHelper.getIntegerOrDefault(config, NUMBER_PER_CHECKPOINT, 5);
  }

  public static String getStateTableName(Map<String, String> config) {
    return ConfigHelper.getStringOrDefault(config, STATE_TABLE_NAME, "table");
  }
}
