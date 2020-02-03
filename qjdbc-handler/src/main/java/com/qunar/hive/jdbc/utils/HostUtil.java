package com.qunar.hive.jdbc.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created with Lee. Date: 2019/9/10 Time: 15:56 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

public class HostUtil {

  public static String getIp(String host) {
    String ip = null;
    try {
      ip = InetAddress.getAllByName(host)[0].getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return ip;
  }

  public static String getHost(String ip) {
    String host = null;
    try {
      host = InetAddress.getAllByName(ip)[0].getHostName();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    return host;
  }
}
