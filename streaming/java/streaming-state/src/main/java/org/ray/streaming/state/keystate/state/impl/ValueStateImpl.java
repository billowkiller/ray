package org.ray.streaming.state.keystate.state.impl;

import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ValueState;

/**
 * ValueState implementation.
 *
 * @author wutao on 2019/7/27
 */
public class ValueStateImpl<T> extends AbstractState<T> implements ValueState<T> {

  public ValueStateImpl(ValueStateDescriptor<T> descriptor, KeyStateBackend backend) {
    super(backend, descriptor);
  }

  @Override
  public void update(T value) {
    this.put(descriptor, value);
  }

  @Override
  public T get() {
    T value = this.get(descriptor);
    if (value == null) {
      return ((ValueStateDescriptor<T>) descriptor).getDefaultValue();
    } else {
      return value;
    }
  }
}
