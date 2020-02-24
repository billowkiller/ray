package org.ray.streaming.state.strategy;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.state.backend.AbstractStateBackend;
import org.ray.streaming.state.backend.BackendType;
import org.ray.streaming.state.backend.KeyStateBackend;
import org.ray.streaming.state.backend.StorageMode;
import org.ray.streaming.state.config.ConfigKey;
import org.ray.streaming.state.keystate.KeyGroup;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.desc.MapStateDescriptor;
import org.ray.streaming.state.keystate.desc.ValueStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;
import org.ray.streaming.state.keystate.state.MapState;
import org.ray.streaming.state.keystate.state.ValueState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Created by eagle on 2019/8/15.
 */
public class CPStateStrategyTest {

  private final String table = "kepler_cp_store";
  private final String defaultValue = "default";
  protected KeyStateBackend keyStateBackend;
  Map<String, String> config = new HashMap<>();
  private String currentTime;

  @BeforeClass
  public void setUp() {
    config.put(ConfigKey.STATE_STORAGE_MODE, StorageMode.DUALVERSION.name());
    currentTime = Long.toString(System.currentTimeMillis());
  }

  public void caseKV() {
    ValueStateDescriptor<String> valueStateDescriptor = ValueStateDescriptor
      .build("value-" + currentTime, String.class, defaultValue);
    valueStateDescriptor.setTableName(table);
    ValueState<String> state = this.keyStateBackend.getValueState(valueStateDescriptor);

    this.keyStateBackend.setBatchId(1l);

    state.setCurrentKey("1");
    state.update("hello");
    state.setCurrentKey("2");
    state.update("world");

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), "hello");
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(), "world");

    this.keyStateBackend.finish(1);

    this.keyStateBackend.setBatchId(2);
    state.setCurrentKey(("3"));
    state.update("eagle");
    state.setCurrentKey(("4"));
    state.update("alex");

    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), "eagle");
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), "alex");

    this.keyStateBackend.commit(1, "");
    this.keyStateBackend.ackCommit(1, 1);

    this.keyStateBackend.finish(2);
    this.keyStateBackend.setBatchId(3);

    state.setCurrentKey(("1"));
    state.update("tim");
    state.setCurrentKey(("4"));
    state.update("scala");

    this.keyStateBackend.finish(3);
    this.keyStateBackend.setBatchId(4);

    state.setCurrentKey(("3"));
    state.update("cook");
    state.setCurrentKey(("2"));
    state.update("inf");

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), "tim");
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(), "inf");
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), "cook");
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), "scala");

    this.keyStateBackend.commit(2, "");
    this.keyStateBackend.ackCommit(2, 2);

    //do rollback 所有内存的都没有了
    this.keyStateBackend.rollBack(2);
    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), defaultValue);
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(), defaultValue);
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), defaultValue);
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), defaultValue);

    this.keyStateBackend.setBatchId(4);
    this.keyStateBackend.setCurrentKey("1");
    state.update("tim");
    this.keyStateBackend.finish(4);

    this.keyStateBackend.setBatchId(5);
    this.keyStateBackend.setCurrentKey("2");
    state.update("info");
    this.keyStateBackend.finish(5);

    this.keyStateBackend.setBatchId(6);
    state.update("cook");
    this.keyStateBackend.finish(6);

    this.keyStateBackend.setBatchId(7);
    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(), "tim");

    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(), "cook");

    //中间的commit和ackCommit可以跳过，没作用

    this.keyStateBackend.commit(5, "");
    this.keyStateBackend.ackCommit(5, 5);

    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(), "tim");

    this.keyStateBackend.rollBack(6);
    this.keyStateBackend.setBatchId(6);

    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(), "tim");
    //2 batchId 6并没有刷到存储, 在rollback之后丢失
    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(), "info");
  }

  public void caseKVGap() {
    ValueStateDescriptor<String> valueStateDescriptor = ValueStateDescriptor
      .build("value2-" + currentTime, String.class, defaultValue);
    valueStateDescriptor.setTableName(table);
    ValueState<String> state = this.keyStateBackend.getValueState(valueStateDescriptor);

    this.keyStateBackend.setBatchId(1L);

    state.setCurrentKey("1");
    state.update("hello");

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), "hello");

    this.keyStateBackend.finish(1);

    this.keyStateBackend.setBatchId(5);
    this.keyStateBackend.setCurrentKey("1");
    state.update("info");
    this.keyStateBackend.finish(5);
    this.keyStateBackend.commit(5, "");
    this.keyStateBackend.ackCommit(5, 5);

    this.keyStateBackend.setBatchId(10);
    Assert.assertEquals(state.get(), "info");
    this.keyStateBackend.finish(10);
    this.keyStateBackend.commit(10, "");
    this.keyStateBackend.ackCommit(10, 10);

    this.keyStateBackend.setBatchId(15);
    state.update("world");
    this.keyStateBackend.finish(15);
    this.keyStateBackend.commit(15, "");
    this.keyStateBackend.ackCommit(15, 15);

    this.keyStateBackend.setBatchId(11);
    this.keyStateBackend.rollBack(11);
    Assert.assertEquals(state.get(), "info");

    this.keyStateBackend.setBatchId(15);
    state.update("world2");
    this.keyStateBackend.finish(15);
    this.keyStateBackend.commit(15, "");
    this.keyStateBackend.ackCommit(15, 15);

    this.keyStateBackend.setBatchId(11);
    this.keyStateBackend.rollBack(11);
    Assert.assertEquals(state.get(), "info");

  }

  public void caseKList() {
    ListStateDescriptor<Integer> listStateDescriptor = ListStateDescriptor
      .build("list-" + currentTime, Integer.class);
    listStateDescriptor.setTableName(table);
    ListState<Integer> state = this.keyStateBackend.getListState(listStateDescriptor);

    this.keyStateBackend.setBatchId(1l);

    state.setCurrentKey("1");
    state.add(1);
    state.setCurrentKey("2");
    state.add(2);

    state.setCurrentKey("1");
    List<Integer> result = state.get();
    Assert.assertEquals(result, Arrays.asList(1));
    state.setCurrentKey("2");
    Assert.assertEquals(state.get(), Arrays.asList(2));

    this.keyStateBackend.finish(1);

    this.keyStateBackend.setBatchId(2);
    state.setCurrentKey(("3"));
    state.add(3);
    state.setCurrentKey(("4"));
    state.add(4);

    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), Arrays.asList(3));
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), Arrays.asList(4));

    this.keyStateBackend.commit(1, "");
    this.keyStateBackend.ackCommit(1, 1);

    this.keyStateBackend.finish(2);
    this.keyStateBackend.setBatchId(3);

    state.setCurrentKey(("1"));
    state.add(2);
    state.setCurrentKey(("4"));
    state.add(5);

    this.keyStateBackend.finish(3);
    this.keyStateBackend.setBatchId(4);

    state.setCurrentKey(("3"));
    state.add(4);
    state.setCurrentKey(("2"));
    state.add(3);

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), Arrays.asList(1, 2));
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(), Arrays.asList(2, 3));
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), Arrays.asList(3, 4));
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), Arrays.asList(4, 5));

    this.keyStateBackend.commit(2, "");
    this.keyStateBackend.ackCommit(2, 2);

    //do rollback 所有内存的都没有了
    this.keyStateBackend.rollBack(2);
    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(), Lists.newArrayList());
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(), Lists.newArrayList());
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(), Lists.newArrayList());
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(), Lists.newArrayList());

    this.keyStateBackend.setBatchId(4);
    this.keyStateBackend.setCurrentKey("1");
    state.add(1);
    this.keyStateBackend.finish(4);

    this.keyStateBackend.setBatchId(5);
    this.keyStateBackend.setCurrentKey("2");
    state.add(2);
    this.keyStateBackend.finish(5);

    this.keyStateBackend.setBatchId(6);
    state.add(3);
    this.keyStateBackend.finish(6);

    this.keyStateBackend.setBatchId(7);
    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(), Arrays.asList(1));

    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(), Arrays.asList(2, 3));

    //中间的commit和ackCommit可以跳过，没作用

    this.keyStateBackend.commit(5, "");
    this.keyStateBackend.ackCommit(5, 5);

    this.keyStateBackend.rollBack(5);

    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(), Arrays.asList(1));
    //2 batchId 6并没有刷到存储, 在rollback之后丢失
    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(), Arrays.asList(2));
  }

  public void caseKMap() {
    MapStateDescriptor<Integer, Integer> mapStateDescriptor = MapStateDescriptor
      .build("map-" + currentTime, Integer.class, Integer.class);
    mapStateDescriptor.setTableName(table);
    MapState<Integer, Integer> state = this.keyStateBackend.getMapState(mapStateDescriptor);

    this.keyStateBackend.setBatchId(1l);

    state.setCurrentKey("1");
    state.put(1, 1);
    state.setCurrentKey("2");
    state.put(2, 2);

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(1), Integer.valueOf(1));
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(2), Integer.valueOf(2));

    this.keyStateBackend.finish(1);

    this.keyStateBackend.setBatchId(2);
    state.setCurrentKey(("3"));
    state.put(3, 3);
    state.setCurrentKey(("4"));
    state.put(4, 4);

    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(3), Integer.valueOf(3));
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(4), Integer.valueOf(4));

    this.keyStateBackend.commit(1, "");
    this.keyStateBackend.ackCommit(1, 1);

    this.keyStateBackend.finish(2);
    this.keyStateBackend.setBatchId(3);

    state.setCurrentKey(("1"));
    state.put(5, 5);
    state.setCurrentKey(("4"));
    state.put(6, 6);

    this.keyStateBackend.finish(3);
    this.keyStateBackend.setBatchId(4);

    state.setCurrentKey(("3"));
    state.put(7, 7);
    state.setCurrentKey(("2"));
    state.put(8, 8);

    state.setCurrentKey(("1"));
    Assert.assertEquals(state.get(1), Integer.valueOf(1));
    Assert.assertEquals(state.get(5), Integer.valueOf(5));
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.get(2), Integer.valueOf(2));
    Assert.assertEquals(state.get(8), Integer.valueOf(8));
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.get(3), Integer.valueOf(3));
    Assert.assertEquals(state.get(7), Integer.valueOf(7));
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.get(4), Integer.valueOf(4));
    Assert.assertEquals(state.get(6), Integer.valueOf(6));

    this.keyStateBackend.commit(2, "");
    this.keyStateBackend.ackCommit(2, 2);

    //do rollback 所有内存的都没有了
    this.keyStateBackend.rollBack(2);
    state.setCurrentKey(("1"));
    Assert.assertEquals(state.entries(), (new HashMap()).entrySet());
    state.setCurrentKey(("2"));
    Assert.assertEquals(state.entries(), (new HashMap()).entrySet());
    state.setCurrentKey(("3"));
    Assert.assertEquals(state.entries(), (new HashMap()).entrySet());
    state.setCurrentKey(("4"));
    Assert.assertEquals(state.entries(), (new HashMap()).entrySet());

    this.keyStateBackend.setBatchId(4);
    this.keyStateBackend.setCurrentKey("1");
    state.put(1, 1);
    this.keyStateBackend.finish(4);

    this.keyStateBackend.setBatchId(5);
    this.keyStateBackend.setCurrentKey("2");
    state.put(2, 2);
    this.keyStateBackend.finish(5);

    this.keyStateBackend.setBatchId(6);
    state.put(3, 3);
    this.keyStateBackend.finish(6);

    this.keyStateBackend.setBatchId(7);
    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(1), Integer.valueOf(1));

    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(2), Integer.valueOf(2));
    Assert.assertEquals(state.get(3), Integer.valueOf(3));

    //中间的commit和ackCommit可以跳过，没作用

    this.keyStateBackend.commit(5, "");
    this.keyStateBackend.ackCommit(5, 5);

    this.keyStateBackend.rollBack(5);

    this.keyStateBackend.setCurrentKey("1");
    Assert.assertEquals(state.get(1), Integer.valueOf(1));
    //2 batchId 6并没有刷到存储, 在rollback之后丢失
    this.keyStateBackend.setCurrentKey("2");
    Assert.assertEquals(state.get(2), Integer.valueOf(2));
    Assert.assertEquals(state.get(3), null);
  }

  @Test
  public void testMem() {
    config.put(ConfigKey.STATE_BACKEND_TYPE, BackendType.MEMORY.name());
    this.keyStateBackend = AbstractStateBackend.buildStateBackend(config)
      .createKeyedStateBackend("test", 10, new KeyGroup(1, 3));
    caseKV();
    caseKVGap();
    caseKList();
    caseKMap();
  }
}