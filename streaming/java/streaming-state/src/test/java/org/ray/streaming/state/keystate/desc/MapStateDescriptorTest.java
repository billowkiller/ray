package org.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/13.
 */
public class MapStateDescriptorTest {

  @Test
  public void test() {
    MapStateDescriptor<String, Integer> descriptor = MapStateDescriptor
      .build("msdTest", String.class, Integer.class);

    descriptor.setTableName("table");
    Assert.assertEquals(descriptor.getTableName(), "table");
    Assert.assertEquals(descriptor.getName(), "msdTest");
  }

}