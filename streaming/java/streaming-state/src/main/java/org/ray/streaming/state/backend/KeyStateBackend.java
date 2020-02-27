package org.ray.streaming.state.backend;

import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.KeyGroupAssignment;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;
import org.ray.streaming.state.keystate.state.facade.ListStateFacade;
import org.ray.streaming.state.keystate.state.facade.MapStateFacade;
import org.ray.streaming.state.keystate.state.facade.ValueStateFacade;

/**
 * key state backend manager, managing different kinds of states.
 */
public class KeyStateBackend extends TransactionKeyStateBackend {

  protected final int numberOfKeyGroups;
  protected final KeyGroup keyGroup;

  public KeyStateBackend(int numberOfKeyGroups, KeyGroup keyGroup,
                         AbstractStateBackend abstractStateBackend) {
    super(abstractStateBackend);
    this.numberOfKeyGroups = numberOfKeyGroups;
    this.keyGroup = keyGroup;
  }

  /**
   * value State
   */
  protected <T> ValueStateFacade<T> newValueStateFacade(ValueStateDescriptor<T> stateDescriptor) {
    return new ValueStateFacade<>(this, stateDescriptor);
  }

  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (valueStateMngMap.containsKey(desc)) {
      return valueStateMngMap.get(desc).getValueState();
    } else {
      ValueStateFacade<T> valueStateFacade = newValueStateFacade(stateDescriptor);
      valueStateMngMap.put(desc, valueStateFacade);
      return valueStateFacade.getValueState();
    }
  }

  /**
   * list State
   */
  protected <T> ListStateFacade<T> newListStateFacade(ListStateDescriptor<T> stateDescriptor) {
    return new ListStateFacade<>(this, stateDescriptor);
  }

  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (listStateMngMap.containsKey(desc)) {
      ListStateFacade<T> listStateFacade = listStateMngMap.get(desc);
      return listStateFacade.getListState();
    } else {
      ListStateFacade<T> listStateFacade = newListStateFacade(stateDescriptor);
      listStateMngMap.put(desc, listStateFacade);
      return listStateFacade.getListState();
    }
  }

  /**
   * map state
   */
  protected <S, T> MapStateFacade<S, T> newMapStateFacade(
      MapStateDescriptor<S, T> stateDescriptor) {
    return new MapStateFacade<>(this, stateDescriptor);
  }

  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    String desc = stateDescriptor.getIdentify();
    if (mapStateMngMap.containsKey(desc)) {
      MapStateFacade<S, T> mapStateFacade = mapStateMngMap.get(desc);
      return mapStateFacade.getMapState();
    } else {
      MapStateFacade<S, T> mapStateFacade = newMapStateFacade(stateDescriptor);
      mapStateMngMap.put(desc, mapStateFacade);
      return mapStateFacade.getMapState();
    }
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    super.keyGroupIndex = KeyGroupAssignment.assignToKeyGroup(currentKey, numberOfKeyGroups);
    super.currentKey = currentKey;
  }

  public int getNumberOfKeyGroups() {
    return numberOfKeyGroups;
  }

  public KeyGroup getKeyGroup() {
    return keyGroup;
  }

  public void close() {
    for (ValueStateFacade facade : valueStateMngMap.values()) {
      facade.close();
    }
    for (ListStateFacade facade : listStateMngMap.values()) {
      facade.close();
    }
    for (MapStateFacade facade : mapStateMngMap.values()) {
      facade.close();
    }
  }
}
