package org.ray.streaming.state.keystate.state.facade;

import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ValueState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ValueStateFacadeTest extends StateFacadeTest {

  ValueStateFacade<Integer> facade;

  @BeforeClass
  public void setUp() {
    ValueStateDescriptor<Integer> descriptor = ValueStateDescriptor
      .build("value", Integer.class, 0);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    facade = new ValueStateFacade<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    ValueState<Integer> state = facade.getValueState();
    Assert.assertEquals(state.get().intValue(), 0);

    facade.put("key1", 2);
    Assert.assertEquals(facade.get("key1").intValue(), 2);

    facade.put("key1", 3);
    Assert.assertEquals(facade.get("key1").intValue(), 3);

    facade.put("key2", 9);
    Assert.assertEquals(facade.get("key2").intValue(), 9);

    facade.put("key2", 6);
    Assert.assertEquals(facade.get("key2").intValue(), 6);
  }
}