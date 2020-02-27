package org.ray.streaming.state.config;

/**
 * Config Exception.
 */
public class ConfigException extends RuntimeException {

  public ConfigException(Throwable t) {
    super(t);
  }

  public ConfigException(String msg) {
    super(msg);
  }
}
