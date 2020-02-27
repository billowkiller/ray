package org.ray.streaming.state.keystate.desc;


import org.ray.streaming.state.keystate.state.ListState;

/**
 * ListStateDescriptor.
 */
public class ListStateDescriptor<T> extends AbstractStateDescriptor<ListState<T>, T> {

  private boolean isOperatorList;
  private int index;
  private int number;

  private ListStateDescriptor(String name, Class<T> type, boolean isOperatorList) {
    super(name, type);
    this.isOperatorList = isOperatorList;
  }

  public static <T> ListStateDescriptor<T> build(String name, Class<T> type) {
    return build(name, type, false);
  }

  public static <T> ListStateDescriptor<T> build(String name, Class<T> type,
                                                 boolean isOperatorList) {
    return new ListStateDescriptor<>(name, type, isOperatorList);
  }

  public boolean isOperatorList() {
    return isOperatorList;
  }

  public void setOperatorList(boolean operatorList) {
    isOperatorList = operatorList;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public int getNumber() {
    return number;
  }

  public void setNumber(int number) {
    this.number = number;
  }

  @Override
  public DescType getDescType() {
    return DescType.list;
  }

  @Override
  public String getIdentify() {
    if (isOperatorList) {
      return String.format("%s-%d-%d", super.getIdentify(), number, index);
    } else {
      return super.getIdentify();
    }
  }
}
