package org.ray.streaming.state.config;

/**
 * @author wutao on 2019/5/11.
 */
public class ConfigException extends RuntimeException {

  public ConfigException(Throwable t) {
    super(t);
  }

  public ConfigException(String msg) {
    super(msg);
  }
}
