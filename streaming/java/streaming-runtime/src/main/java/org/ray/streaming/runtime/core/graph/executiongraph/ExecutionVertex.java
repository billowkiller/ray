package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical vertex, correspond to {@link ExecutionJobVertex}.
 */
public class ExecutionVertex implements Serializable {

  /**
   * Unique id for execution vertex.
   */
  private final int vertexId;

  /**
   * Ordered index for execution vertex.
   */
  private final int vertexIndex;

  /**
   * Unique name generated by vertex name and index for execution vertex.
   */
  private final String vertexName;

  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;
  private RayActor<JobWorker> workerActor;
  private List<ExecutionEdge> inputEdges = new ArrayList<>();
  private List<ExecutionEdge> outputEdges = new ArrayList<>();

  public ExecutionVertex(int jobVertexId, int index, ExecutionJobVertex executionJobVertex) {
    this.vertexId = generateExecutionVertexId(jobVertexId, index);
    this.vertexIndex = index;
    this.vertexName = executionJobVertex.getJobVertexName() + "-" + vertexIndex;
  }

  private int generateExecutionVertexId(int jobVertexId, int index) {
    return jobVertexId * 100000 + index;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }

  public ExecutionVertexState getState() {
    return state;
  }

  public void setState(ExecutionVertexState state) {
    this.state = state;
  }

  public boolean is2Add() {
    return state == ExecutionVertexState.TO_ADD;
  }

  public boolean isRunning() {
    return state == ExecutionVertexState.RUNNING;
  }

  public boolean is2Delete() {
    return state == ExecutionVertexState.TO_DEL;
  }

  public RayActor<JobWorker> getWorkerActor() {
    return workerActor;
  }

  public ActorId getWorkerActorId() {
    return workerActor.getId();
  }

  public void setWorkerActor(RayActor<JobWorker> workerActor) {
    this.workerActor = workerActor;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public String getVertexName() {
    return vertexName;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("vertexIndex", vertexIndex)
        .add("vertexName", vertexName)
        .add("state", state)
        .add("workerActor", workerActor)
        .add("inputEdges", inputEdges)
        .add("outputEdges", outputEdges)
        .toString();
  }
}
