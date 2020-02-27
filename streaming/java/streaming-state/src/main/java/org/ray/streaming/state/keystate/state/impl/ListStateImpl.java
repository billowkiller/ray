package org.ray.streaming.state.keystate.state.impl;

import java.util.ArrayList;
import java.util.List;
import org.ray.streaming.state.backend.TransactionKeyStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;

/**
 * ListState implementation.
 */
public class ListStateImpl<V> extends AbstractState<List<V>> implements ListState<V> {

  public ListStateImpl(ListStateDescriptor<V> descriptor, TransactionKeyStateBackend backend) {
    super(backend, descriptor);
  }

  @Override
  public List<V> get() {
    List<V> list = this.get(descriptor);
    if (list == null) {
      list = new ArrayList<>();
    }
    return list;
  }

  @Override
  public void add(V value) {
    List<V> list = this.get(descriptor);
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(value);
    this.put(descriptor, list);
  }

  @Override
  public void update(List<V> list) {
    if (list == null) {
      list = new ArrayList<>();
    }
    this.put(descriptor, list);
  }
}
