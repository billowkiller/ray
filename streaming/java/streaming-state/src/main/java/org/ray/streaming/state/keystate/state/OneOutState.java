package org.ray.streaming.state.keystate.state;

/**
 * @author wutao
 * @date 2019/7/25
 */
public interface OneOutState<O> extends State {

  /**
   * get the value in state
   *
   * @return the value in state
   */
  O get();
}
