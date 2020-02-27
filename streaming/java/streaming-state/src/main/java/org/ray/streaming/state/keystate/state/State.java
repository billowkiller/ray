package org.ray.streaming.state.keystate.state;

/**
 * State interface.
 */
public interface State {

  /**
   * set current key of the state
   */
  void setCurrentKey(Object currentKey);
}
