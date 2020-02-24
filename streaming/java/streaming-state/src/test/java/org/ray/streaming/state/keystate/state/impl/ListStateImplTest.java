package org.ray.streaming.state.keystate.state.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.impl.MemoryStateBackend;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2019/8/18.
 */
public class ListStateImplTest {

  ListStateImpl<Integer> listState;
  KeyStateBackend keyStateBackend;

  @BeforeClass
  public void setUp() throws Exception {
    keyStateBackend = new KeyStateBackend(1, new KeyGroup(1, 2),
      new MemoryStateBackend(new HashMap<>()));
    ListStateDescriptor<Integer> descriptor = ListStateDescriptor
      .build("ListStateImplTest", Integer.class);
    descriptor.setTableName("table");

    listState = (ListStateImpl<Integer>) keyStateBackend.getListState(descriptor);
  }

  @Test
  public void testAddGet() throws Exception {
    keyStateBackend.setContext(1L, 1);
    List<Integer> list = listState.get();
    Assert.assertEquals(list.size(), 0);

    listState.add(1);
    listState.add(2);

    Assert.assertEquals(listState.get(), Arrays.asList(1, 2));

    listState.add(3);
    Assert.assertEquals(listState.get(), Arrays.asList(1, 2, 3));

    list = listState.get();
    list.set(1, -1);
    listState.add(4);
    Assert.assertEquals(listState.get(), Arrays.asList(1, -1, 3, 4));

    keyStateBackend.setCurrentKey(2);

    listState.add(5);
    listState.add(6);

    Assert.assertEquals(listState.get(), Arrays.asList(5, 6));
  }


  @Test(dependsOnMethods = {"testAddGet"})
  public void testUpdate() throws Exception {
    Assert.assertEquals(listState.get(), Arrays.asList(5, 6));

    listState.update(Arrays.asList(7, 8, 9));

    List<Integer> list = listState.get();
    Assert.assertEquals(list, Arrays.asList(7, 8, 9));

    list.set(1, 10);
    listState.update(list);
    Assert.assertEquals(list, Arrays.asList(7, 10, 9));

    listState.update(null);
    Assert.assertEquals(listState.get().size(), 0);
  }
}