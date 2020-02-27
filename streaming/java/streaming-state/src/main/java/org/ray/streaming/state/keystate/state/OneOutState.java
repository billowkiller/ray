package org.ray.streaming.state.keystate.state;

/**
 * one value per state.
 */
public interface OneOutState<O> extends State {

  /**
   * get the value in state
   *
   * @return the value in state
   */
  O get();
}
