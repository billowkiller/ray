package org.ray.streaming.state.keystate.desc;

import org.testng.Assert;
import org.testng.annotations.Test;

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