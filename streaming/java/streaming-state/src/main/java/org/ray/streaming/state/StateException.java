package org.ray.streaming.state;

/**
 * RuntimeException wrapper.
 */
public class StateException extends RuntimeException {

  public StateException(Throwable t) {
    super(t);
  }

  public StateException(String msg) {
    super(msg);
  }
}
