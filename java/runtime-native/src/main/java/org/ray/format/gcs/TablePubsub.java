package org.ray.format.gcs;
// automatically generated by the FlatBuffers compiler, do not modify

public final class TablePubsub {
  private TablePubsub() { }
  public static final int NO_PUBLISH = 0;
  public static final int TASK = 1;
  public static final int RAYLET_TASK = 2;
  public static final int CLIENT = 3;
  public static final int OBJECT = 4;
  public static final int ACTOR = 5;
  public static final int HEARTBEAT = 6;

  public static final String[] names = { "NO_PUBLISH", "TASK", "RAYLET_TASK", "CLIENT", "OBJECT", "ACTOR", "HEARTBEAT", };

  public static String name(int e) { return names[e]; }
}

