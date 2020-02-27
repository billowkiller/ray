package org.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ValueStateDescriptorTest {

  @Test
  public void test() {
    ValueStateDescriptor<Integer> descriptor = ValueStateDescriptor
      .build("vsdTest", Integer.class, 0);
    Assert.assertEquals(descriptor.getDefaultValue().intValue(), 0);
  }
}