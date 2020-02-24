package org.ray.streaming.state.keystate.state;

/**
 * @author wutao
 * @date 2019/7/25
 */
public interface AggregatingState<I, O> extends OneOutState<O> {

  /**
   * add the value
   *
   * @param value the new value
   */
  void add(I value);
}
