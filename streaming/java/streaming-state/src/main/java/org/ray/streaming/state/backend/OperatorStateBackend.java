package org.ray.streaming.state.backend;

import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.facade.ListStateFacade;
import org.ray.streaming.state.keystate.state.impl.OperatorStateImpl;

/**
 * OperatorState manager.
 */
public class OperatorStateBackend extends TransactionKeyStateBackend {

  public OperatorStateBackend(AbstractStateBackend backend) {
    super(backend);
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    super.currentKey = currentKey;
  }

  protected <T> ListStateFacade<T> newListStateFacade(ListStateDescriptor<T> stateDescriptor) {
    return new ListStateFacade<>(this, stateDescriptor);
  }

  /**
   * splitList
   */
  public <T> ListState<T> getSplitListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listStateMngMap.containsKey(desc)) {
      ListStateFacade<T> listStateFacade = listStateMngMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateFacade<T> listStateFacade = newListStateFacade(stateDescriptor);
      listStateMngMap.put(desc, listStateFacade);
      ((OperatorStateImpl) (listStateFacade.getListState())).setSplit(true);
      return listStateFacade.getListState();
    }
  }

  /**
   * unionList
   */
  public <T> ListState<T> getUnionListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listStateMngMap.containsKey(desc)) {
      ListStateFacade<T> listStateFacade = listStateMngMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateFacade<T> listStateFacade = newListStateFacade(stateDescriptor);
      listStateMngMap.put(desc, listStateFacade);
      ((OperatorStateImpl) (listStateFacade.getListState())).init();
      return listStateFacade.getListState();
    }
  }
}
