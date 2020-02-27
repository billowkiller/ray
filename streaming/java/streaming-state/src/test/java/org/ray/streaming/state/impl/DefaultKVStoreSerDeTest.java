package org.ray.streaming.state.impl;

import org.ray.streaming.state.serde.impl.DefaultKVStoreSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultKVStoreSerDeTest {

  DefaultKVStoreSerDe<String, Integer> serDe = new DefaultKVStoreSerDe<>();
  byte[] ret;

  @Test
  public void testSerKey() throws Exception {
    ret = serDe.serKey("key");
    String key = new String(ret);
    Assert.assertEquals(key.indexOf("key"), 5);
  }

  @Test
  public void testSerValue() throws Exception {
    ret = serDe.serValue(5);
    Assert.assertEquals(ret.length, 2);
    Assert.assertEquals((int) serDe.deSerValue(ret), 5);
  }
}