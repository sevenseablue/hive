package com.qunar.hive.jdbc.authority;

import com.qunar.hive.jdbc.authority.entry.AuthorityInfo;
import com.qunar.hive.jdbc.conf.DatabaseType;
import com.qunar.hive.jdbc.conf.JdbcStorageConfig;
import com.qunar.hive.jdbc.conf.JdbcStorageConfigManager;
import com.qunar.hive.jdbc.dao.PxcJdbcDatabaseAccessor;
import com.qunar.hive.jdbc.dao.QmhaJdbcDatabaseAccessor;
import com.qunar.hive.jdbc.exception.HiveJdbcAuthorityException;
import com.qunar.hive.jdbc.utils.HostUtil;
import com.qunar.hive.jdbc.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created with Lee. Date: 2019/9/9 Time: 17:22 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

public class AuthorityManager {

  private static final LogUtil LOGGER = LogUtil.getLogger();
  private static final Pattern PATTERM = Pattern.compile("jdbc:.*?:\\/\\/(.*?):(.*)?\\/(.*)");

  public static String getAuthority(Configuration conf) throws HiveJdbcAuthorityException {
    String tableName = JdbcStorageConfigManager.getTableName(conf);
    String username = JdbcStorageConfig.USERNAME.getConfigValue(conf);
    String password = JdbcStorageConfig.PASSWORD.getConfigValue(conf);
    String database = "";
    String namespace = "";
    AuthorityInfo authorityInfo = null;
    try {
      DatabaseType databaseType = DatabaseType
          .valueOf(JdbcStorageConfig.DATABASE_TYPE.getConfigValue(conf));
      switch (databaseType) {
        case MYSQLQMHA:
          database = conf.get(QmhaJdbcDatabaseAccessor.QMHA_CONFIG_PREFIX + ".dbname");
          namespace = conf.get(QmhaJdbcDatabaseAccessor.QMHA_CONFIG_PREFIX + ".namespace");
          LOGGER.info(String
              .format("pararm is : namespace:%s,database:%s,tablename:%s,username:%s,password:%s",
                  namespace, database, tableName, username, password));
          authorityInfo = new AuthorityInfo(username, password, namespace, database, tableName);
          break;
        case MYSQLPXC:
          database = conf.get(PxcJdbcDatabaseAccessor.PXC_CONFIG_PREFIX + ".dbname");
          namespace = conf.get(PxcJdbcDatabaseAccessor.PXC_CONFIG_PREFIX + ".namespace");
          LOGGER.info(String
              .format("pararm is : namespace:%s,database:%s,tablename:%s,username:%s,password:%s",
                  namespace, database, tableName, username, password));
          authorityInfo = new AuthorityInfo(username, password, namespace, database, tableName);
          break;
        default:
          String ip = "";
          String port = "";
          String url = JdbcStorageConfig.JDBC_URL.getConfigValue(conf);
          Matcher matcher = PATTERM.matcher(url);
          if (matcher.find()) {
            ip = matcher.group(1);
            ip = HostUtil.getIp(ip);
            port = matcher.group(2);
            database = matcher.group(3);
          }
          LOGGER.info(String
              .format("pararm is : ip:%s,port:%s,database:%s,tablename:%s,username:%s,password:%s",
                  ip, port, database, tableName, username, password));
          authorityInfo = new AuthorityInfo(username, password, ip, port, database, tableName);
      }

      List<AuthorityInfo> authorityInfos = new ArrayList<>();
      authorityInfos.addAll(AuthorityDao.query(AuthJdbc.getConnection(), authorityInfo));
      authorityInfo.setTableName("*");
      authorityInfos.addAll(AuthorityDao.query(AuthJdbc.getConnection(), authorityInfo));
      return String.join(",", authorityInfos.stream().flatMap(x-> Arrays.stream(x.getPower().split(","))).collect(Collectors.toSet()));
    } catch (HiveJdbcAuthorityException e) {
      LOGGER.info("get authority failed:", e);
      throw new HiveJdbcAuthorityException(e.getMessage());
    }
  }

}
