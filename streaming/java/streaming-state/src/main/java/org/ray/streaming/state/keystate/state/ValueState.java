package org.ray.streaming.state.keystate.state;

/**
 * ValueState interface.
 */
public interface ValueState<T> extends OneOutState<T> {

  /**
   * update the value
   *
   * @param value the new value
   */
  void update(T value);
}
