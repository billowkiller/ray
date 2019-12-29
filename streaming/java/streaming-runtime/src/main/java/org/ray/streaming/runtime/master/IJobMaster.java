package org.ray.streaming.runtime.master;

/**
 * Job master.
 */
public interface IJobMaster {

  /**
   * Init job master.
   * @param isRecover true: when in failover
   * @return
   */
  Boolean init(boolean isRecover);
}