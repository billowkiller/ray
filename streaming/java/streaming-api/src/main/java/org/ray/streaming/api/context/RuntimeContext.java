package org.ray.streaming.api.context;

import java.util.Map;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;

/**
 * Encapsulate the runtime information of a streaming task.
 */
public interface RuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  Long getBatchId();

  void setBatchId(long batchId);

  Long getMaxBatch();

  Map<String, String> getConfig();

  //-------STATE-------
  void setCurrentKey(Object key);

  KeyStateBackend getKeyStateManager();

  <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor);

  <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor);

  <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor);
  //-------END-------

}
