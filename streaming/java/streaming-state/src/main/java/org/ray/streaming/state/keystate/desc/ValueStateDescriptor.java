package org.ray.streaming.state.keystate.desc;

import org.ray.streaming.state.keystate.state.ValueState;

/**
 * ValueStateDescriptor.
 */
public class ValueStateDescriptor<T> extends AbstractStateDescriptor<ValueState<T>, T> {

  private T defaultValue;

  public ValueStateDescriptor(String name, Class<T> type, T defaultValue) {
    super(name, type);
    this.defaultValue = defaultValue;
  }

  public static <T> ValueStateDescriptor<T> build(String name, Class<T> type, T defaultValue) {
    return new ValueStateDescriptor<>(name, type, defaultValue);
  }

  public T getDefaultValue() {
    return defaultValue;
  }

  @Override
  public DescType getDescType() {
    return DescType.value;
  }
}
