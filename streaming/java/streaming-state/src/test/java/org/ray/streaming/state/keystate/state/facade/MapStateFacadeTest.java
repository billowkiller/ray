package org.ray.streaming.state.keystate.state.facade;

import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MapStateFacadeTest extends StateFacadeTest {

  MapStateFacade<String, Integer> facade;

  @BeforeClass
  public void setUp() {
    MapStateDescriptor<String, Integer> descriptor = MapStateDescriptor
      .build("map", String.class, Integer.class);
    descriptor.setTableName("tableName");
    keyStateBackend.setContext(1L, "key");
    facade = new MapStateFacade<>(keyStateBackend, descriptor);
  }

  @Test
  public void test() throws Exception {
    Assert.assertEquals(facade.getMapState().get().size(), 0);

    Map<String, Integer> map = new HashMap<>();
    map.put("key1", 1);
    map.put("key2", 2);
    facade.put("key1", map);
    Assert.assertEquals(facade.get("key1"), map);

    map.remove("key1");
    facade.put("key2", map);
    Assert.assertEquals(facade.get("key2"), map);
  }

}