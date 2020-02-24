package org.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/13.
 */
public class ListStateDescriptorTest {

  @Test
  public void test() {
    ListStateDescriptor<Integer> descriptor = ListStateDescriptor
      .build("lsdTest", Integer.class, true);
    descriptor.setTableName("table");
    Assert.assertTrue(descriptor.isOperatorList());

    descriptor.setNumber(3);
    descriptor.setIndex(0);

    Assert.assertEquals(descriptor.getNumber(), 3);
    Assert.assertEquals(descriptor.getIndex(), 0);

    Assert.assertEquals(descriptor.getIdentify(), "lsdTest-3-0");
  }
}