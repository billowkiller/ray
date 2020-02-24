package org.ray.streaming.state.keystate;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class defines keygroup assignment, referring to flink.
 */
public final class KeyGroupAssignment {

  /**
   * Computes the range of key-groups that are assigned to a given operator under the given
   * parallelism and maximum
   * parallelism.
   *
   * @param maxParallelism Maximal parallelism that the job was initially created with.
   * @param parallelism The current parallelism under which the job runs. Must be <=
   *     maxParallelism.
   * @param index Id of a key-group. 0 <= keyGroupID < maxParallelism.
   */
  public static KeyGroup computeKeyGroupRangeForOperatorIndex(int maxParallelism, int parallelism,
                                                              int index) {

    Preconditions.checkArgument(maxParallelism >= parallelism,
        "Maximum parallelism (%s) must not be smaller than parallelism(%s)", maxParallelism,
        parallelism);

    int start = index == 0 ? 0 : ((index * maxParallelism - 1) / parallelism) + 1;
    int end = ((index + 1) * maxParallelism - 1) / parallelism;
    return new KeyGroup(start, end);
  }

  /**
   * Assigns the given key to a key-group index.
   *
   * @param key the key to assign
   * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
   * @return the key-group to which the given key is assigned
   */
  public static int assignToKeyGroup(Object key, int maxParallelism) {
    return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
  }

  /**
   * Assigns the given key to a key-group index.
   *
   * @param keyHash the hash of the key to assign
   * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
   * @return the key-group to which the given key is assigned
   */
  public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
    // we can rehash keyHash
    return Math.abs(keyHash % maxParallelism);
  }

  public static Map<Integer, List<Integer>> computeKeyGroupToTask(int maxParallelism,
                                                                  List<Integer> targetTasks) {
    Map<Integer, List<Integer>> keyGroupToTask = new ConcurrentHashMap<>();
    for (int index = 0; index < targetTasks.size(); index++) {
      KeyGroup taskKeyGroup = computeKeyGroupRangeForOperatorIndex(maxParallelism,
          targetTasks.size(), index);
      for (int groupId = taskKeyGroup.getStartKeyGroup(); groupId <= taskKeyGroup.getEndKeyGroup();
          groupId++) {
        keyGroupToTask.put(groupId, ImmutableList.of(targetTasks.get(index)));
      }
    }
    return keyGroupToTask;
  }

}
