package org.ray.streaming.state.impl;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.store.IKMapStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Created by eagle on 2019/8/14.
 */
public class MemoryKMapStoreTest {

  private AbstractStateBackend stateBackend;
  private IKMapStore<String, String, String> ikMapStore;

  @BeforeClass
  public void setUp() {
    stateBackend = AbstractStateBackend.buildStateBackend(new HashMap<String, String>());
    ikMapStore = stateBackend.getKeyMapStore("test-table");
  }

  @Test
  public void testCase() {
    try {
      Assert.assertEquals(ikMapStore.get("hello"), null);
      Map<String, String> map = Maps.newHashMap();
      map.put("1", "1-1");
      map.put("2", "2-1");

      ikMapStore.put("hello", map);
      Assert.assertEquals(ikMapStore.get("hello"), map);

      Map<String, String> map2 = Maps.newHashMap();
      map.put("3", "3-1");
      map.put("4", "4-1");
      ikMapStore.put("hello", map2);
      Assert.assertNotEquals(ikMapStore.get("hello"), map);
      Assert.assertEquals(ikMapStore.get("hello"), map2);


    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
