package org.ray.streaming.state.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.store.IKVStore;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MemoryKVStoreTest {

  private AbstractStateBackend stateBackend;
  private IKVStore<String, String> ikvStore;

  @BeforeClass
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    stateBackend = AbstractStateBackend.buildStateBackend(config);
    ikvStore = stateBackend.getKeyValueStore("kepler_hlg_ut");
  }

  @Test
  public void testCase() {
    try {
      ikvStore.put("hello", "world");
      Assert.assertEquals(ikvStore.get("hello"), "world");
      ikvStore.put("hello", "world1");
      Assert.assertEquals(ikvStore.get("hello"), "world1");
      Assert.assertEquals(ikvStore.get("hello1"), null);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
