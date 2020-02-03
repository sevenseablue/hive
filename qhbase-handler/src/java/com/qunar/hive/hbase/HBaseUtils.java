package com.qunar.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.util.ArrayList;


public class HBaseUtils {
    private static String HADOOP_HOME = System.getenv("HADOOP_HOME");
    private static String HBASE_HOME = System.getenv("HBASE_HOME");

    public static Configuration getConf() {
        Configuration conf = null;
        conf = HBaseConfiguration.create();
        conf.set("fs.viewfs.impl", org.apache.hadoop.fs.viewfs.ViewFileSystem.class.getName());
        conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/core-site.xml"));
        conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/hdfs-site.xml"));
        conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/mountTable.xml"));
        conf.addResource(new Path(HBASE_HOME+"/conf/hbase-site.xml"));
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
        for(int i = 1; i < keys.length; ++i) {
//            HiveKey hiveKey = new HiveKey();
//            hiveKey.set(new BytesWritable(keys[i]));
//            writer.append(hiveKey, nullValue);
            writer.append(new HiveKey(keys[i], i), nullValue);
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseUtils.writePartitionFile(HBaseUtils.getConf(), "/user/corphive/hive/tmp/part/t1.part.2", "datadev:t2");
    }
}
