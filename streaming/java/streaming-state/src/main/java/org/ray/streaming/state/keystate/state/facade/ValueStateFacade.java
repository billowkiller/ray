package org.ray.streaming.state.keystate.state.facade;

import org.ray.streaming.state.IKVState;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ValueState;
import org.ray.streaming.state.keystate.state.impl.ValueStateImpl;
import org.ray.streaming.state.strategy.AbstractTransactionState;

/**
 * This class defines ValueState Wrapper, connecting state and backend.
 */
public class ValueStateFacade<T> extends AbstractTransactionState<T> implements
    IKVState<String, T> {

  private KeyStateBackend keyStateBackend;
  private ValueStateImpl<T> valueState;

  public ValueStateFacade(KeyStateBackend keyStateBackend,
                          ValueStateDescriptor<T> stateDescriptor) {
    super(keyStateBackend.getBackStorage(stateDescriptor), keyStateBackend.getStorageMode());
    this.keyStateBackend = keyStateBackend;
    this.valueState = new ValueStateImpl<>(stateDescriptor, keyStateBackend);
  }

  public ValueState<T> getValueState() {
    return this.valueState;
  }

  @Override
  public T get(String key) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    return this.stateStrategy.get(this.keyStateBackend.getBatchId(), key, valueState);
  }

  @Override
  public void put(String key, T t) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    this.stateStrategy.put(this.keyStateBackend.getBatchId(), key, t);
  }

}
