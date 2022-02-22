package com.alibaba.datax.plugin.reader.mongodbreader.util;

import java.security.MessageDigest;

/**
 * @author cong.yu
 * @date 2019-12-23 10:19
 */
public class Md5Utils {

  private static String byteArrayToHexString(byte[] b) {
    StringBuffer resultSb = new StringBuffer();
    for (int i = 0; i < b.length; i++) {
      resultSb.append(byteToHexString(b[i]));
    }

    return resultSb.toString();
  }

  private static String byteToHexString(byte b) {
    int n = b;
    if (n < 0) {
      n += 256;
    }
    int d1 = n / 16;
    int d2 = n % 16;
    return hexDigits[d1] + hexDigits[d2];
  }

  /**
   * 返回大写MD5
   */
  private static String MD5Encode(String origin, String charsetname) {
    String resultString = null;
    try {
      resultString = new String(origin);
      MessageDigest md = MessageDigest.getInstance("MD5");
      if (charsetname == null || "".equals(charsetname)) {
        resultString = byteArrayToHexString(md.digest(resultString.getBytes()));
      } else {
        resultString = byteArrayToHexString(md.digest(resultString.getBytes(charsetname)));
      }
      return resultString.toUpperCase();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String MD5EncodeUtf8(String origin) {
//    origin = origin + PropertiesUtil.getProperty("password.salt", "");
    return MD5Encode(origin, "utf-8");
  }


  private static final String hexDigits[] = {"0", "1", "2", "3", "4", "5",
      "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

}
