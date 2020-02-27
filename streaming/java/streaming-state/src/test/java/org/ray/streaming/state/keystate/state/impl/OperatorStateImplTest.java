package org.ray.streaming.state.keystate.state.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.state.backend.OperatorStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

public class OperatorStateImplTest {

  OperatorStateImpl<Integer> operatorState;
  ListStateDescriptor<Integer> descriptor;
  OperatorStateBackend operatorStateBackend;

  @BeforeClass
  public void setUp() throws Exception {
    String table_name = "kepler_hbase_operator";
    Map config = new HashMap<>();

    operatorStateBackend = new OperatorStateBackend(new MemoryStateBackend(config));

    descriptor = ListStateDescriptor
      .build("OperatorStateImplTest" + System.currentTimeMillis(), Integer.class, true);
    descriptor.setNumber(1);
    descriptor.setIndex(0);
    descriptor.setTableName(table_name);

    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);
  }

  //@Test
  public void testInit() throws Exception {
    operatorStateBackend.setBatchId(1L);
    // 第一次启动
    List<Integer> list = operatorState.get();
    Assert.assertEquals(list.size(), 0);

    for (int i = 0; i < 100; i++) {
      operatorState.add(i);
    }
    Assert.assertEquals(operatorState.get().size(), 100);
    operatorStateBackend.finish(1L);
    operatorStateBackend.commit(1L, "");
    operatorStateBackend.ackCommit(1L, 0);

    operatorStateBackend.finish(5L);
    operatorStateBackend.commit(5L, "");
    operatorStateBackend.ackCommit(5L, 0);

    /**
     * assume last time we has only one executor
     * and save 100 values, now we have 3
     */

    /** at the view of index 2 */
    descriptor.setNumber(3);
    descriptor.setIndex(2);
    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);

    operatorStateBackend.setBatchId(1L);
    Assert.assertEquals(operatorState.get().size(), 33);

    /** at the view of index 1 */
    descriptor.setIndex(1);
    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);

    operatorStateBackend.setBatchId(2L);
    Assert.assertEquals(operatorState.get().size(), 33);

    for (int i = 0; i < 100; i++) {
      operatorState.add(i);
    }
    Assert.assertEquals(operatorState.get().size(), 133);

    /** at the view of index 0 */
    descriptor.setIndex(0);
    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);

    operatorStateBackend.setBatchId(2L);
    Assert.assertEquals(operatorState.get().size(), 34);

    operatorStateBackend.finish(2L);
    operatorStateBackend.commit(2L, "");
    operatorStateBackend.ackCommit(2L, 0);
    /**
     * save 100 more values, now we only have 2
     * at the view of index 1
     */
    descriptor.setIndex(1);
    descriptor.setNumber(2);
    operatorState = (OperatorStateImpl<Integer>) operatorStateBackend.getSplitListState(descriptor);

    operatorStateBackend.setBatchId(3L);
    Assert.assertEquals(operatorState.get().size(), 100);
  }
}