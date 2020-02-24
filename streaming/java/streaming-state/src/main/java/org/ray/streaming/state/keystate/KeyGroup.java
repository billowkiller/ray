package org.ray.streaming.state.keystate;

import com.google.common.base.Preconditions;
import java.io.Serializable;

/**
 * This class defines key-groups, referring to flink. Key-groups are the granularity into which
 * the key space of a job, is partitioned for keyed state-handling in state backends. The
 * boundaries of the range are inclusive.
 */
public class KeyGroup implements Serializable {

  private final int startKeyGroup;
  private final int endKeyGroup;

  /**
   * Defines the range [startKeyGroup, endKeyGroup]
   *
   * @param startKeyGroup start of the range (inclusive)
   * @param endKeyGroup end of the range (inclusive)
   */
  public KeyGroup(int startKeyGroup, int endKeyGroup) {
    Preconditions.checkArgument(startKeyGroup >= 0);
    Preconditions.checkArgument(startKeyGroup <= endKeyGroup);
    this.startKeyGroup = startKeyGroup;
    this.endKeyGroup = endKeyGroup;
    Preconditions.checkArgument(getNumberOfKeyGroups() >= 0, "Potential overflow detected.");
  }

  /**
   * @return The number of key-groups in the range
   */
  public int getNumberOfKeyGroups() {
    return 1 + endKeyGroup - startKeyGroup;
  }

  public int getStartKeyGroup() {
    return startKeyGroup;
  }

  public int getEndKeyGroup() {
    return endKeyGroup;
  }

  @Override
  public String toString() {
    return "KeyGroup{" + "startKeyGroup=" + startKeyGroup + ", endKeyGroup=" + endKeyGroup + '}';
  }
}
