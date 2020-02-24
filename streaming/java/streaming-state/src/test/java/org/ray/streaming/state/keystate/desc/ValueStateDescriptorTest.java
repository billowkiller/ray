package org.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/13.
 */
public class ValueStateDescriptorTest {

  @Test
  public void test() {
    ValueStateDescriptor<Integer> descriptor = ValueStateDescriptor
      .build("vsdTest", Integer.class, 0);
    Assert.assertEquals(descriptor.getDefaultValue().intValue(), 0);
  }
}