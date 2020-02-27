package org.ray.streaming.runtime.worker.context;

import static org.ray.streaming.util.Config.STREAMING_BATCH_MAX_COUNT;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.OperatorStateBackend;
import org.ray.streaming.state.backend.TransactionKeyStateBackend;
import org.ray.streaming.state.keystate.desc.AbstractStateDescriptor;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;

/**
 * Use Ray to implement RuntimeContext.
 */
public class RayRuntimeContext implements RuntimeContext {
  private int taskId;
  private int taskIndex;
  private int parallelism;
  private Long batchId;
  private final Long maxBatch;
  private Map<String, String> config;
  /**
   * Backend for keyed state. This might be empty if we're not on a keyed stream.
   */
  protected transient KeyStateBackend keyStateManager;
  /**
   * Backend for operator state. This might be empty
   */
  protected transient OperatorStateBackend operatorStateManager;


  public RayRuntimeContext(ExecutionTask executionTask, Map<String, String> config,
      int parallelism) {
    this.taskId = executionTask.getTaskId();
    this.config = config;
    this.taskIndex = executionTask.getTaskIndex();
    this.parallelism = parallelism;
    if (config.containsKey(STREAMING_BATCH_MAX_COUNT)) {
      this.maxBatch = Long.valueOf(config.get(STREAMING_BATCH_MAX_COUNT));
    } else {
      this.maxBatch = Long.MAX_VALUE;
    }
  }

  @Override
  public int getTaskId() {
    return taskId;
  }

  @Override
  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getBatchId() {
    return batchId;
  }

  @Override
  public void setCheckpointId(long checkpointId) {
    if (this.keyStateManager != null) {
      this.keyStateManager.setBatchId(checkpointId);
    }
    if (this.operatorStateManager != null) {
      this.operatorStateManager.setBatchId(checkpointId);
    }
    this.batchId = checkpointId;
  }

  @Override
  public Long getMaxBatch() {
    return maxBatch;
  }

  @Override
  public Map<String, String> getConfig() {
    return config;
  }

  @Override
  public void setCurrentKey(Object key) {
    this.keyStateManager.setCurrentKey(key);
  }

  @Override
  public KeyStateBackend getKeyStateManager() {
    return keyStateManager;
  }

  public void setKeyStateManager(KeyStateBackend keyStateManager) {
    this.keyStateManager = keyStateManager;
  }

  @Override
  public <T> ValueState<T> getValueState(ValueStateDescriptor<T> stateDescriptor) {
    stateSanityAndPresetAndSetMetrics(stateDescriptor, this.keyStateManager);
    return this.keyStateManager.getValueState(stateDescriptor);
  }

  @Override
  public <T> ListState<T> getListState(ListStateDescriptor<T> stateDescriptor) {
    stateSanityAndPresetAndSetMetrics(stateDescriptor, this.keyStateManager);
    return this.keyStateManager.getListState(stateDescriptor);
  }

  @Override
  public <S, T> MapState<S, T> getMapState(MapStateDescriptor<S, T> stateDescriptor) {
    stateSanityAndPresetAndSetMetrics(stateDescriptor, this.keyStateManager);
    return this.keyStateManager.getMapState(stateDescriptor);
  }

  protected void stateSanityAndPresetAndSetMetrics(AbstractStateDescriptor stateDescriptor,
                                                   TransactionKeyStateBackend backend) {
    Preconditions.checkNotNull(stateDescriptor, "The state properties must not be null");
    Preconditions.checkNotNull(backend, "keyState must not be null");
  }
}
