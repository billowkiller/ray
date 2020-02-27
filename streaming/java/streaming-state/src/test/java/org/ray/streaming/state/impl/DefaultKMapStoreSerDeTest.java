package org.ray.streaming.state.impl;

import java.util.HashMap;
import java.util.Map;
import org.ray.streaming.state.serde.impl.DefaultKMapStoreSerDe;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DefaultKMapStoreSerDeTest {

  private DefaultKMapStoreSerDe<String, String, Map<String, String>> defaultKMapStoreSerDe;

  @BeforeClass
  public void setUp() {
    this.defaultKMapStoreSerDe = new DefaultKMapStoreSerDe<>();
  }

  @Test
  public void testSerKey() {
    String key = "hello";
    byte[] result = this.defaultKMapStoreSerDe.serKey(key);
    String keyWithPrefix = this.defaultKMapStoreSerDe.generateRowKeyPrefix(key.toString());
    Assert.assertEquals(result, keyWithPrefix.getBytes());
  }

  @Test
  public void testSerUKey() {
    String subKey = "hell1";
    byte[] result = this.defaultKMapStoreSerDe.serUKey(subKey);
    Assert.assertEquals(subKey, this.defaultKMapStoreSerDe.deSerUKey(result));
  }

  @Test
  public void testSerUValue() {
    Map<String, String> value = new HashMap<>();
    value.put("foo", "bar");
    byte[] result = this.defaultKMapStoreSerDe.serUValue(value);
    Assert.assertEquals(value, this.defaultKMapStoreSerDe.deSerUValue(result));
  }

}