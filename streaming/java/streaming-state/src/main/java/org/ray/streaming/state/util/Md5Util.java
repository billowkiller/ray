package org.ray.streaming.state.util;

import com.twmacinta.util.MD5;
import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Md5 utils for hbase rowkey
 */
public class Md5Util {

  public static final char[] hexChar = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'a', 'b', 'c', 'd', 'e', 'f'};
  private static final Logger LOGGER = LoggerFactory.getLogger(Md5Util.class);

  private Md5Util() {
  }

  public static String md5sum(byte[] b) {
    MD5 md5 = new MD5();
    md5.Update(b);
    return md5.asHex();
  }


  public static String md5sum(String str) {
    if (StringUtils.isNotEmpty(str)) {
      try {
        byte[] bytes = str.getBytes("utf-8");
        return md5sum(bytes);
      } catch (UnsupportedEncodingException e) {
        LOGGER.error("Md5 fail.", e);
      }
      return md5sum(str.getBytes());
    }
    return "";
  }
}
