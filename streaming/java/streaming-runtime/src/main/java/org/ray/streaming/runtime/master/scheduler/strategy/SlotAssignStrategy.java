package org.ray.streaming.runtime.master.scheduler.strategy;

import java.util.List;
import java.util.Map;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;

public interface SlotAssignStrategy {

  /**
   * Calucate slot number per container and set to resources.
   * @param containers
   * @param maxParallelism
   */
  int getSlotNumPerContainer(List<Container> containers, int maxParallelism);

  /**
   * Allocate slot to container
   * @param containers
   * @param slotNumPerContainer
   * @return
   */
  Map<Container, List<Slot>> allocateSlot(final List<Container> containers,
      final int slotNumPerContainer);

  /**
   * Assign slot to execution vertex
   *
   * @param executionGraph execution graph
   * @param containerSlotsMap container -> slots map
   * @param containerResource container resources
   * @return HashMap, key: container address, value: HashMap (key: slotId, value: opName)
   */
  Map<String, Map<Integer, List<String>>> assignSlot(ExecutionGraph executionGraph,
      final Map<Container, List<Slot>> containerSlotsMap,
      final Map<Container, Map<String, Double>> containerResource);

  /**
   * Rebalance allocating map
   *
   * @param executionJobVertex execution job vertex
   * @param containerSlotsMap container -> slots map
   * @param containerResource container resources
   * @return HashMap, key: container address, value: HashMap (key: slotId, value: opName)
   */
  Map<String, Map<Integer, List<String>>> rebalance(ExecutionJobVertex executionJobVertex,
      Map<Container, List<Slot>> containerSlotsMap,
      Map<Container, Map<String, Double>> containerResource);

  /**
   * Get slot assign strategy name
   */
  String getName();

  /**
   * Get resources from resource manager.
   * @param resources
   */
  void setResources(Resources resources);
}