package com.qunar.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotExistsException;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;


public class HBaseUtils {
  private static String HADOOP_HOME = System.getenv("HADOOP_HOME");
  private static String HBASE_HOME = System.getenv("HBASE_HOME");

  public static Configuration getConf() {
    Configuration conf = null;
    conf = HBaseConfiguration.create();
    conf.set("fs.viewfs.impl", org.apache.hadoop.fs.viewfs.ViewFileSystem.class.getName());
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/core-site.xml"));
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path(HADOOP_HOME + "/etc/hadoop/mountTable.xml"));
    conf.addResource(new Path(HBASE_HOME + "/conf/hbase-site.xml"));
    return conf;
  }

  public static byte[][] getTabKeys(String tabName) throws IOException {
    return getTabKeys(getConf(), tabName);
  }

  public static byte[][] getTabKeys(Configuration conf, String tabName) throws IOException {
    Configuration config = HBaseConfiguration.create(conf);
    HTable hTable = new HTable(config, tabName);
    byte[][] startKeys = hTable.getRegionLocator().getStartKeys();
    hTable.close();
    return startKeys;
  }

  public static int getRegionNum(Configuration conf, String tabName) throws IOException {
    return getTabKeys(conf, tabName).length;
  }

  public static void delIfExistHfile(Configuration conf, String file) throws IOException {
    String hfilePath = conf.get(Constant.HFILE_FAMILY_PATH);
    if (hfilePath!=null && hfilePath.length()>5 && file.contains(hfilePath)){
      Path dst = new Path(file);
      FileSystem fs = dst.getFileSystem(conf);
      if (fs.exists(dst)) {
        fs.delete(dst, false);
      }
    }
  }

  public static void writePartitionFile(Configuration conf, String partitionFile, String tabName)
      throws IOException {

    byte[][] keys = getTabKeys(conf, tabName);
    Path dst = new Path(partitionFile);
    FileSystem fs = dst.getFileSystem(conf);
    if (fs.exists(dst)) {
      fs.delete(dst, false);
    }
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs,
//            conf, dst, HiveKey.class, NullWritable.class, SequenceFile.CompressionType.valueOf(conf.get("mapred.output.compression.type")), new GzipCodec());
//        SequenceFile.Writer writer = SequenceFile.createWriter(fs,
//            conf, dst, HiveKey.class, NullWritable.class, SequenceFile.CompressionType.NONE, new GzipCodec());
    SequenceFile.Writer writer = SequenceFile.createWriter(fs,
        conf, dst, HiveKey.class, NullWritable.class, SequenceFile.CompressionType.valueOf(conf.get("mapred.output.compression.type")));
    NullWritable nullValue = NullWritable.get();
    for (int i = 1; i < keys.length; ++i) {
//            HiveKey hiveKey = new HiveKey();
//            hiveKey.set(new BytesWritable(keys[i]));
//            writer.append(hiveKey, nullValue);
      writer.append(new HiveKey(keys[i], i), nullValue);
    }
    writer.close();
  }

  public static void createSnapshot(String snapshotname, String name) throws IOException {
    createSnapshot(snapshotname, name, getConf());
  }

  public static void createSnapshot(String snapshotName, String name, Configuration conf) {
    TableName tableName = TableName.valueOf(name);
    Connection conn = null;
    Admin admin = null;
    SessionState.LogHelper console = SessionState.getConsole();
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        admin.snapshot(snapshotName, tableName);
        console.printInfo("snapshot " + snapshotName + " created");
      } else {
        console.printInfo("hbase table " + name + " not exists!");
      }

    } catch (SnapshotExistsException e) {
      console.printInfo("snapshot " + snapshotName + " already exist");
      console.printInfo("snapshot " + snapshotName + " deleting");
      try {
        admin.deleteSnapshot(snapshotName);
        console.printInfo("snapshot " + snapshotName + " deleted");
        admin.snapshot(snapshotName, tableName);
        console.printInfo("snapshot " + snapshotName + " created");
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void deleteSnapshot(String snapshotName, Configuration conf) {
    Connection conn = null;
    Admin admin = null;
    SessionState.LogHelper console = SessionState.getConsole();
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
      admin.deleteSnapshot(snapshotName);
      console.printInfo("snapshot " + snapshotName + " deleted");
    } catch (SnapshotDoesNotExistException e) {
      console.printInfo("snapshot " + snapshotName + " does not exist");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (admin != null) {
        try {
          admin.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void checkConfArgument(boolean exp, @Nullable Object errorMessage) {
    if (exp) {
      throw new NullPointerException(String.valueOf(errorMessage));
    }
  }

  /**
   * Generate the hfile unique directory from the hbase table
   *
   * @param hfileprepath
   * @param hbaseName
   * @return
   */
  public static String getHfilePath(String hfileprepath, String hbaseName) {
    hbaseName = hbaseName.contains(":") ? hbaseName.replace(":", "/") : Constant.HBASE_DEFAULT_PRE + hbaseName;
    String hfilepath = null;
    if (!hfileprepath.endsWith(Constant.PATH_DELIMITER)) {
      hfilepath = hfileprepath + Constant.PATH_DELIMITER + hbaseName;
    } else {
      hfilepath = hfileprepath + hbaseName;
    }
    return hfilepath;
  }

  public static void setConf(Configuration jc, String name, String value) {
    jc.set(name, value);
  }

  /**
   * Check whether the parameters are configured,Must be configured "hfile.family.name",Otherwise it can't query
   *
   * @param jc         job configuration
   * @param tableProps
   * @return
   */
  public static String getHfileFamilyPath(Configuration jc, Properties tableProps) {
    String hfileprepath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    checkConfArgument(
        hfileprepath == null,
        "Please set " + Constant.HFILE_FAMILY_PATH + " to target location for HFiles");
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    checkConfArgument(
        hbaseName == null,
        "Please set " + Constant.HBASE_TABLE_NAME + " to target hbase table");
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    checkConfArgument(
        familyName == null,
        "Please set " + Constant.HBASE_TABLE_FAMILY_NAME + " to target hbase table family name");

    String hfilePath = getHfilePath(hfileprepath, hbaseName);
    setConf(jc, Constant.BULKLOAD_HFILE_PATH, hfilePath);

    return hfilePath + Constant.PATH_DELIMITER + familyName;
  }

  /**
   * Set the temporary directory of the hfile intermediate file
   *
   * @param jc
   * @param tableProps
   * @return
   */
  public static String getFinalOutPath(Configuration jc, Properties tableProps) {
    String hfilePrePath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    return getHfilePath(hfilePrePath + "/__hfile_out_tmp/", hbaseName+"_"+familyName);
  }

  public static String getPartitionFilePath(Configuration jc, Properties tableProps) {
    String hfilePrePath = jc.get(Constant.HFILE_FAMILY_PATH, tableProps.getProperty(Constant.HFILE_FAMILY_PATH));
    String hbaseName = tableProps.getProperty(Constant.HBASE_TABLE_NAME);
    String familyName = tableProps.getProperty(Constant.HBASE_TABLE_FAMILY_NAME);
    return getHfilePath(hfilePrePath + "/__partition_file_path__/", hbaseName+"_"+familyName+"/000000_0");
  }

  public static void main(String[] args) throws IOException {
    String partsFile = args[1];
    String tableName = args[0];
    HBaseUtils.writePartitionFile(HBaseUtils.getConf(), partsFile, tableName);
  }
}
