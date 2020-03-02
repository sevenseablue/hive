package com.qunar.hive.hbase;

import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

public class HBaseTotalOrderPartitioner implements Partitioner<HiveKey, Object>, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTotalOrderPartitioner.class);

  private Partitioner<HiveKey, Object> partitioner;

  @Override
  public void configure(JobConf job) {
    if (partitioner == null) {
      configurePartitioner(new JobConf(job));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (partitioner == null) {
      configurePartitioner(new JobConf(conf));
    }
  }

  @Override
  public int getPartition(HiveKey key, Object value, int numPartitions) {
    return partitioner.getPartition(key, value, numPartitions);
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  private void configurePartitioner(JobConf conf) {
    LOG.info(TotalOrderPartitioner.getPartitionFile(conf));
    partitioner = new TotalOrderPartitioner<HiveKey, Object>();
    partitioner.configure(conf);
  }
}
