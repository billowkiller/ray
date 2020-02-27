package org.ray.streaming.state.keystate.desc;

import com.google.common.base.Preconditions;
import org.ray.streaming.state.keystate.state.State;

/**
 * This class defines basic data structures of StateDescriptor.
 */
public abstract class AbstractStateDescriptor<S extends State, T> {

  private String tableName;
  private String name;
  private Class<T> type;

  protected AbstractStateDescriptor(String name, Class<T> type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public Class<T> getType() {
    return type;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public abstract DescType getDescType();

  public String getIdentify() {
    Preconditions.checkArgument(this.tableName != null, "table name must not be null.");
    Preconditions.checkArgument(this.name != null, "table name must not be null.");
    return this.name;
  }

  @Override
  public String toString() {
    return "AbstractStateDescriptor{" + "tableName='" + tableName + '\'' + ", name='" + name + '\''
      + ", type=" + type + '}';
  }

  public enum DescType {
    /**
     * value state
     */
    value,

    /**
     * list state
     */
    list,

    /**
     * map state
     */
    map
  }
}
