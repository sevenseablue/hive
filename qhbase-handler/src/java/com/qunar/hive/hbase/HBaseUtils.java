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

import java.io.IOException;


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
    Configuration config = HBaseUtils.getConf();
    HTable hTable = new HTable(config, tabName);
    byte[][] startKeys = hTable.getRegionLocator().getStartKeys();
    hTable.close();
    return startKeys;
  }

  public static void writePartitionFile(Configuration conf, String partitionFile, String tabName)
      throws IOException {

    byte[][] keys = getTabKeys(tabName);
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

  public static void deleteSnapshot(String snapshotName, String name, Configuration conf) {
    TableName tableName = TableName.valueOf(name);
    Connection conn = null;
    Admin admin = null;
    SessionState.LogHelper console = SessionState.getConsole();
    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        admin.deleteSnapshot(snapshotName);
        console.printInfo("snapshot " + snapshotName + " deleted");
      } else {
        console.printInfo("hbase table " + name + " not exists!");
      }

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

  public static void main(String[] args) throws IOException {
    HBaseUtils.writePartitionFile(HBaseUtils.getConf(), "/user/corphive/hive/tmp/part/t1.part.2", "datadev:t2");
  }
}
