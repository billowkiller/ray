package org.ray.streaming.state.keystate.state;

/**
 * State interface.
 *
 * @author wutao on 2019/7/25
 */
public interface State {

  /**
   * set current key of the state
   */
  void setCurrentKey(Object currentKey);
}
