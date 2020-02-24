package org.ray.streaming.state.keystate.state.impl;

import java.util.HashMap;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2019/8/18.
 */
public class ValueStateImplTest {

  ValueStateImpl<String> valueState;
  KeyStateBackend keyStateBackend;

  @BeforeClass
  public void setUp() throws Exception {
    keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 2),
      new MemoryStateBackend(new HashMap<>()));
    ValueStateDescriptor<String> descriptor = ValueStateDescriptor
      .build("ValueStateImplTest", String.class, "hello");
    descriptor.setTableName("table");

    valueState = (ValueStateImpl<String>) keyStateBackend.getValueState(descriptor);
  }

  @Test
  public void testUpdateGet() throws Exception {
    keyStateBackend.setContext(1L, 1);

    Assert.assertEquals(valueState.get(), "hello");

    String str = valueState.get();

    valueState.update(str + " world");
    Assert.assertEquals(valueState.get(), "hello world");
  }
}