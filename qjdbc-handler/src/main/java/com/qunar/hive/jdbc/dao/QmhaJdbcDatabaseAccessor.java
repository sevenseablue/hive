package com.qunar.hive.jdbc.dao;

import com.qunar.db.resource.BasicDataSource;
import com.qunar.db.resource.MasterDelegatorDataSource;
import com.qunar.db.resource.SlaveDelegatorDataSource;
import com.qunar.db.resource.impl.TomcatJdbcDataSourceFactory;
import com.qunar.hive.jdbc.conf.JdbcStorageConfig;
import com.qunar.hive.jdbc.utils.Constant;
import org.apache.hadoop.conf.Configuration;

/**
 * Created with Lee. Date: 2019/8/20 Time: 16:11 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

public class QmhaJdbcDatabaseAccessor extends MySqlDatabaseAccessor {

  public static final String QMHA_CONFIG_PREFIX = Constant.MYSQL_QMHA_PREFIX;
  protected boolean write = false;

  @Override
  protected void initializeDatabaseConnection(Configuration conf) {
    if (dataSource == null) {
      synchronized (this) {
        if (dataSource == null) {
          this.dataSource = getDataSource(conf);
        }
      }
    }
  }

  private BasicDataSource getDataSource(Configuration conf) {
    TomcatJdbcDataSourceFactory tomcatJdbcDataSourceFactory = new TomcatJdbcDataSourceFactory();
    //初始化连接池配置
    String namespace = conf.get(QMHA_CONFIG_PREFIX + ".namespace");
    //String username = conf.get(QMHA_CONFIG_PREFIX + ".username");
    //String password = conf.get(QMHA_CONFIG_PREFIX + ".password");
    String username = JdbcStorageConfig.USERNAME.getUserName(conf);
    String password = JdbcStorageConfig.PASSWORD.getPassword(conf);
    String dbname = conf.get(QMHA_CONFIG_PREFIX + ".dbname");
    String corepoolsize = conf.get(QMHA_CONFIG_PREFIX + ".corepoolsize", "1");
    String maxpoolsize = conf.get(QMHA_CONFIG_PREFIX + ".maxpoolsize", "1");
    String jdbcurloption = conf
        .get(QMHA_CONFIG_PREFIX + ".jdbcurloption", "?useunicode=true&amp;autoReconnect=true");

    LOGGER.info(
        "namespace:{};username:{},password:{},dbname:{},corepoolsize:{},maxpoolsize:{},jdbcurloption:{}",
        namespace, username, password, dbname, corepoolsize, maxpoolsize, jdbcurloption);
    BasicDataSource basicDataSource;
    if (write) {
      basicDataSource = new MasterDelegatorDataSource(
          namespace,
          username,
          password,
          dbname,
          Integer.parseInt(corepoolsize),
          Integer.parseInt(maxpoolsize),
          jdbcurloption,
          tomcatJdbcDataSourceFactory
      );
    } else {
      basicDataSource = new SlaveDelegatorDataSource(
          namespace,
          username,
          password,
          dbname,
          Integer.parseInt(corepoolsize),
          Integer.parseInt(maxpoolsize),
          jdbcurloption,
          tomcatJdbcDataSourceFactory
      );
    }
    return basicDataSource;
  }

  public void setWrite(boolean write) {
    this.write = write;
  }

}