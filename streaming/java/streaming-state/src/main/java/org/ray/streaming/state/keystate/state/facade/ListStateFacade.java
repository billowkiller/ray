package org.ray.streaming.state.keystate.state.facade;

import java.util.List;
import org.ray.streaming.state.IKVState;
import org.ray.streaming.state.backend.TransactionKeyStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.impl.AbstractState;
import org.ray.streaming.state.keystate.state.impl.ListStateImpl;
import org.ray.streaming.state.keystate.state.impl.OperatorStateImpl;
import org.ray.streaming.state.strategy.AbstractTransactionState;

/**
 * This class defines ListState Wrapper, connecting state and backend.
 *
 * @author wutao on 2018/8/2.
 */
public class ListStateFacade<T> extends AbstractTransactionState<List<T>> implements
    IKVState<String, List<T>> {

  private TransactionKeyStateBackend keyStateBackend;
  private ListState<T> listState;

  public ListStateFacade(TransactionKeyStateBackend keyStateBackend,
                         ListStateDescriptor<T> stateDescriptor) {
    super(keyStateBackend.getBackStorage(stateDescriptor), keyStateBackend.getStorageMode());
    this.keyStateBackend = keyStateBackend;
    if (stateDescriptor.isOperatorList()) {
      this.listState = new OperatorStateImpl<>(stateDescriptor, keyStateBackend);
    } else {
      this.listState = new ListStateImpl<>(stateDescriptor, keyStateBackend);
    }
  }

  public ListState<T> getListState() {
    return this.listState;
  }

  @Override
  public List<T> get(String key) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    return this.stateStrategy
      .get(this.keyStateBackend.getBatchId(), key, (AbstractState) listState);
  }

  @Override
  public void put(String key, List<T> t) {
    this.stateStrategy.setKeyGroupIndex(keyStateBackend.getKeyGroupIndex());
    this.stateStrategy.put(this.keyStateBackend.getBatchId(), key, t);
  }
}
