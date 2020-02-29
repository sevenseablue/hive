package com.qunar.hive.hbase;


import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Logger;

/**
 * When the hfile is successfully generated, it is moved to hbase in batches
 *
 * @author wanhong.qi
 */
public class QPostExecuteHbaseHandler implements ExecuteWithHookContext {

  private static final Logger LOG = Logger.getLogger(FileSystemUtil.class);

  /**
   * Defines the path of hfile temporary directory
   */
  private String bulkloadHfilePath = null;
  private String hbasetablename = null;
  private String hivetablename = null;
  boolean isbulkload = false;
  boolean iswrite = false;
  boolean ishivehbasehandler = false;

  LogHelper console = SessionState.getConsole();

  @Override
  public void run(HookContext hookContext) throws Exception {
    assert (hookContext.getHookType() == HookType.POST_EXEC_HOOK);

    // delete snapshot
    SessionState sess = SessionState.get();
    Map<String, String> variables = sess.getHiveVariables();
    HiveConf sessionConf = sess.getConf();

    String hbaseHandlerType = "hive.hbase.handler.rwType";
    LOG.info(hbaseHandlerType+"\tvalue="+variables.getOrDefault(hbaseHandlerType, ""));

    if(variables.getOrDefault(hbaseHandlerType, "").equals("read")){
      HBaseUtils.deleteSnapshot(sessionConf.getVar(HiveConf.ConfVars.HIVE_HBASE_SNAPSHOT_NAME), HBaseConfiguration.create(sessionConf));
      LOG.info("delete snapshot success");
      return;
    }

    sessionConf.setIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS, -1);
    sessionConf.setVar(HiveConf.ConfVars.HIVEPARTITIONER, org.apache.hadoop.hive.ql.io.DefaultHivePartitioner.class.getName());

//    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    bulkLoad(hookContext, outputs);

    String peVal = sessionConf.getVar(HiveConf.ConfVars.POSTEXECHOOKS);
    String peValUp = peVal.replaceAll(QPostExecuteHbaseHandler.class.getName() + ",", "");
    sessionConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, peValUp);

    String localAutoVal = variables.get("hive.exec.mode.local.auto.pre");
    sessionConf.setVar(HiveConf.ConfVars.LOCALMODEAUTO, localAutoVal);
  }

  public void bulkLoad(HookContext hookContext, Set<WriteEntity> outputs) throws Exception {
    if (console == null) {
      return;
    }
    getHiveMetaInfo(outputs);
    if (hivetablename != null && iswrite && ishivehbasehandler) {
      getClientProperty(hookContext);
      isbulkload = (bulkloadHfilePath != null) && (hbasetablename != null) ? true : isbulkload;
      if (isbulkload) {
        bulkloadHfilePath = FileSystemUtil.getVlidateHFilePath(bulkloadHfilePath, hookContext);
        doBulkLoad();
      }
    }
  }


  /**
   * Get hive and hbase table info from meta
   *
   * @param outputs This object may be a table, partition, dfs directory or a local directory.
   */
  public void getHiveMetaInfo(Set<WriteEntity> outputs) throws Exception {

    for (WriteEntity we : outputs) {
      WriteEntity.WriteType writeType = we.getWriteType();
      switch (writeType) {
        case INSERT:
        case INSERT_OVERWRITE: {
          iswrite = true;
          break;
        }

        default:
          break;
      }
    }

    if (iswrite) {
      for (WriteEntity we : outputs) {
        ReadEntity.Type typ = we.getType();
        switch (typ) {
          case TABLE: {
            Table t = we.getTable();
            ishivehbasehandler = isHiveHbaseHandler(t);
            hbasetablename = t.getProperty(Constant.HBASE_TABLE_NAME);
            hivetablename = t.getTableName();
            break;
          }
          default:
            break;
        }
      }
    }
  }

  public void doBulkLoad() {
    console.printInfo("bulk load data to hbase...");
    Configuration hconf = HBaseConfiguration.create();
    String[] args = new String[2];
    args[0] = bulkloadHfilePath;
    args[1] = hbasetablename;
    Path path = new Path(bulkloadHfilePath);
    FileSystem fs = null;
    try {
      fs = path.getFileSystem(hconf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    int ret = 0;
    try {
      ret = ToolRunner.run(new LoadIncrementalHFiles(hconf), args);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (ret == 0) {
        try {
          fs.delete(path, true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        LOG.error("bulk load to " + hbasetablename + " faild");
      }
    }
  }


  /**
   * get hfile path from jobconf
   *
   * @param hookContext
   */
  public void getClientProperty(HookContext hookContext) {
    HiveConf conf = hookContext.getConf();
    Properties allProperties = conf.getAllProperties();
    bulkloadHfilePath = allProperties.getProperty(Constant.BULKLOAD_HFILE_PATH);
  }


  /**
   * Returns true iff the table is a HiveHBaseHandler
   *
   * @param t
   * @return
   */
  public boolean isHiveHbaseHandler(Table t) {
    return t.getStorageHandler() instanceof HBaseStorageHandler;
  }
}
