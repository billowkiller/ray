package org.ray.streaming.state.keystate.state.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.ray.streaming.state.PartitionRecord;
import org.ray.streaming.state.backend.TransactionKeyStateBackend;
import org.ray.streaming.state.keystate.desc.ListStateDescriptor;
import org.ray.streaming.state.keystate.state.ListState;

/**
 * This class defines the implementation of operator state.
 * When the state is initialized, we must scan the whole table.
 * And if the state type is splitList, all the records must be spitted.
 *
 * @author wutao on 2018/8/4.
 */
public class OperatorStateImpl<V> extends AbstractState<List<PartitionRecord<V>>>
    implements ListState<V> {

  private List<PartitionRecord<V>> allList;

  private AtomicBoolean hasInit;
  private boolean isSplit;

  public OperatorStateImpl(ListStateDescriptor<V> descriptor, TransactionKeyStateBackend backend) {
    super(backend, descriptor);
    this.isSplit = false;
    this.hasInit = new AtomicBoolean(false);
    this.allList = new ArrayList<>();
  }

  private void splitList() {
    // fetch target list and save
    List<PartitionRecord<V>> list = new ArrayList<>();
    int step = ((ListStateDescriptor) descriptor).getNumber();
    assert (step > 0);

    for (int round = 0; round * step <= allList.size(); round++) {
      int m = round * step + ((ListStateDescriptor) descriptor).getIndex();
      if (m < allList.size()) {
        PartitionRecord<V> tmp = allList.get(m);
        tmp.setPartitionNum(((ListStateDescriptor) descriptor).getNumber());
        list.add(tmp);
      }
    }
    this.put(descriptor, list);
    allList.clear();
  }

  private void scan() {
    int partitionNum = -1;
    int index = 0;
    while (true) {
      List<PartitionRecord<V>> list = backend
          .get(descriptor, getKey(descriptor.getIdentify(), index));
      if (list != null && !list.isEmpty()) {
        partitionNum = list.get(0).getPartitionNum();
        allList.addAll(list);
      }
      if (++index >= partitionNum) {
        break;
      }
    }
  }

  public void init() {
    scan();

    if (isSplit) {
      splitList();
    }
  }

  private String getKey(String descName, int index) {
    String[] stringList = descName.split("-");
    return String.format("%s-%s-%d", stringList[0], stringList[1], index);
  }

  @Override
  protected String getStateKey(String descName) {
    return getKey(descName, ((ListStateDescriptor) this.descriptor).getIndex());
  }

  @Override
  public void setCurrentKey(Object currentKey) {
    throw new UnsupportedOperationException("OperatorState cannot set current key");
  }

  @Override
  public List<V> get() {
    if (!hasInit.getAndSet(true)) {
      init();
    }
    List<PartitionRecord<V>> prList = this.get(descriptor);
    List<V> list = new ArrayList<>();
    for (PartitionRecord<V> pr : prList) {
      list.add(pr.getValue());
    }
    return list;
  }

  @Override
  public void add(V value) {
    if (!hasInit.getAndSet(true)) {
      init();
    }
    List<PartitionRecord<V>> list = this.get(descriptor);
    if (list == null) {
      list = new ArrayList<>();
    }
    list.add(new PartitionRecord<>(((ListStateDescriptor) descriptor).getNumber(), value));
    this.put(descriptor, list);
  }

  @Override
  public void update(List<V> list) {
    List<PartitionRecord<V>> prList = new ArrayList<>();
    if (list != null) {
      for (V value : list) {
        prList.add(new PartitionRecord<>(((ListStateDescriptor) descriptor).getNumber(), value));
      }
    }
    this.put(descriptor, prList);
  }

  public void setSplit(boolean split) {
    this.isSplit = split;
  }
}
