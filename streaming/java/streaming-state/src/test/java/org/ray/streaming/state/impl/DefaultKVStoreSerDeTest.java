package org.ray.streaming.state.impl;

import org.ray.streaming.state.serde.impl.DefaultKVStoreSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author wutao on 2017/9/12.
 */
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