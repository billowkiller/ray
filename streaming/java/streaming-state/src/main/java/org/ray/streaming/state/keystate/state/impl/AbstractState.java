package org.ray.streaming.state.keystate.state.impl;

import com.google.common.base.Preconditions;
import org.ray.streaming.state.backend.TransactionKeyStateBackend;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor;

/**
 * Basic State.
 *
 * @author wutao on 2019/7/27
 */
public abstract class AbstractState<T> {

  protected TransactionKeyStateBackend backend;
  protected AbstractStateDescriptor descriptor;

  public AbstractState(TransactionKeyStateBackend backend, AbstractStateDescriptor descriptor) {
    this.backend = backend;
    this.descriptor = descriptor;
  }

  protected String getStateKey(String descName) {
    Preconditions.checkNotNull(backend, "KeyedBackend must not be null");
    Preconditions.checkNotNull(backend.getCurrentKey(), "currentKey must not be null");
    return this.backend.getBackend().getStateKey(descName, backend.getCurrentKey().toString());
  }

  public void put(AbstractStateDescriptor descriptor, T value) {
    backend.put(descriptor, getStateKey(descriptor.getIdentify()), value);
  }

  public T get(AbstractStateDescriptor descriptor) {
    return backend.get(descriptor, getStateKey(descriptor.getIdentify()));
  }

  public void setCurrentKey(Object currentKey) {
    Preconditions.checkNotNull(backend, "KeyedBackend must not be null");
    this.backend.setCurrentKey(currentKey);
  }

  public void setKeyGroupIndex(int keyGroupIndex) {
    Preconditions.checkNotNull(keyGroupIndex >= 0, "keyGroupIndex must > 0");
    this.backend.setKeyGroupIndex(keyGroupIndex);
  }

  public void resetKeyGroupIndex() {
    this.backend.setKeyGroupIndex(-1);
  }

  public AbstractStateDescriptor getDescriptor() {
    return descriptor;
  }
}
