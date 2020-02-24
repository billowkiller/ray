package org.ray.streaming.state.keystate.state.facade;

import java.util.Arrays;
import java.util.List;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/13.
 */
public class ListStateFacadeTest extends StateFacadeTest {

  ListStateFacade<Integer> facade;

  @BeforeClass
  public void setUp() {
    ListStateDescriptor<Integer> descriptor = ListStateDescriptor.build("list", Integer.class);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    facade = new ListStateFacade<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    Assert.assertEquals(facade.getListState().get().size(), 0);

    List<Integer> list = Arrays.asList(1, 2, 3);
    facade.put("key1", list);
    Assert.assertEquals(facade.get("key1"), list);

    facade.put("key1", Arrays.asList(1, 3));

    facade.put("key2", Arrays.asList(4, 5));
    Assert.assertEquals(facade.get("key2"), Arrays.asList(4, 5));

    Assert.assertEquals(facade.get("key1"), Arrays.asList(1, 3));
  }
}