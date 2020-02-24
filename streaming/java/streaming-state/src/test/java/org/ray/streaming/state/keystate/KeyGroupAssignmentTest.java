package org.ray.streaming.state.keystate;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * Created by eagle on 2019/8/29.
 */
public class KeyGroupAssignmentTest {


  @Test
  public void testComputeKeyGroupRangeForOperatorIndex() throws Exception {
    KeyGroup keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(4096, 1, 0);
    assertEquals(keyGroup.getStartKeyGroup(), 0);
    assertEquals(keyGroup.getEndKeyGroup(), 4095);
    assertEquals(keyGroup.getNumberOfKeyGroups(), 4096);

    KeyGroup keyGroup2 = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(4096, 2, 0);
    assertEquals(keyGroup2.getStartKeyGroup(), 0);
    assertEquals(keyGroup2.getEndKeyGroup(), 2047);
    assertEquals(keyGroup2.getNumberOfKeyGroups(), 2048);

    keyGroup = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(4096, 3, 0);
    assertEquals(keyGroup.getStartKeyGroup(), 0);
    assertEquals(keyGroup.getEndKeyGroup(), 1365);
    assertEquals(keyGroup.getNumberOfKeyGroups(), 1366);

    keyGroup2 = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(4096, 3, 1);
    assertEquals(keyGroup2.getStartKeyGroup(), 1366);
    assertEquals(keyGroup2.getEndKeyGroup(), 2730);
    assertEquals(keyGroup2.getNumberOfKeyGroups(), 1365);

    KeyGroup keyGroup3 = KeyGroupAssignment.computeKeyGroupRangeForOperatorIndex(4096, 3, 2);
    assertEquals(keyGroup3.getStartKeyGroup(), 2731);
    assertEquals(keyGroup3.getEndKeyGroup(), 4095);
    assertEquals(keyGroup3.getNumberOfKeyGroups(), 1365);
  }

}