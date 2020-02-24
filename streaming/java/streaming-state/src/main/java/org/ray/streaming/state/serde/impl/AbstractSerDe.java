package org.ray.streaming.state.serde.impl;

import org.apache.commons.lang3.StringUtils;
import org.ray.streaming.state.StateException;
import org.ray.streaming.state.util.Md5Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by eagle on 2018/8/28.
 */
public abstract class AbstractSerDe {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSerDe.class);

  public String generateRowKeyPrefix(String key) {
    if (StringUtils.isNotEmpty(key)) {
      String md5 = Md5Util.md5sum(key);
      if ("".equals(md5)) {
        throw new StateException("Invalid value to md5:" + key);
      }
      return StringUtils.substring(md5, 0, 4) + ":" + key;
    } else {
      LOGGER.warn("key is empty");
      return key;
    }
  }
}
