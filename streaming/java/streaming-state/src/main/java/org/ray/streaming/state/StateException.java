package org.ray.streaming.state;

/**
 * RuntimeException wrapper.
 *
 * @author wutao on 2019/12/26.
 */
public class StateException extends RuntimeException {

  public StateException(Throwable t) {
    super(t);
  }

  public StateException(String msg) {
    super(msg);
  }
}
