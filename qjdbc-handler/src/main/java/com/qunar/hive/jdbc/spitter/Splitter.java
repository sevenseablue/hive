package com.qunar.hive.jdbc.spitter;

import com.google.common.collect.Lists;
import com.qunar.hive.jdbc.JdbcInputSplit;
import com.qunar.hive.jdbc.conf.JdbcStorageConfigManager;
import com.qunar.hive.jdbc.dao.DatabaseAccessor;
import com.qunar.hive.jdbc.dao.DatabaseAccessorFactory;
import com.qunar.hive.jdbc.exception.HiveJdbcDatabaseAccessException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

/**
 * Created with Lee. Date: 2019/9/18 Time: 11:33 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

public abstract class Splitter implements IntervalSplitter {

  private String lowerBound;
  private String upperBound;
  private String partitionColumn;
  private Integer partitionNum;
  private String size;
  private List<JdbcInputSplit> splits;
  private Configuration conf;
  private Path path;

  public Splitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.partitionColumn = partitionColumn;
    this.partitionNum = partitionNum;
  }

  public Splitter() {
  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public void setPartitionColumn(String partitionColumn) {
    this.partitionColumn = partitionColumn;
  }

  public String getLowerBound() {
    return lowerBound;
  }

  public void setLowerBound(String lowerBound) {
    this.lowerBound = lowerBound;
  }

  public String getUpperBound() {
    return upperBound;
  }

  public void setUpperBound(String upperBound) {
    this.upperBound = upperBound;
  }

  public Integer getPartitionNum() {
    return partitionNum;
  }

  public void setPartitionNum(Integer partitionNum) {
    this.partitionNum = partitionNum;
  }

  public String getSize() {
    return size;
  }

  public void setSize(String size) {
    this.size = size;
  }

  public void setSplits(List<JdbcInputSplit> splits) {
    this.splits = splits;
  }

  public List<JdbcInputSplit> getSplits() {
    return splits;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  @Override
  public void initSplitter(String lowerBound, String upperBound, String partitionColumn,
      Integer partitionNum, Configuration conf) throws HiveJdbcDatabaseAccessException {
    this.setPartitionColumn(partitionColumn);
    this.setPartitionNum(partitionNum);
    this.setLowerBound(lowerBound);
    this.setUpperBound(upperBound);
    this.setConf(conf);
    splits = Lists.newArrayList();
    path = FileInputFormat.getInputPaths((JobConf) conf)[0];
    //如果有为null的默认放一个map去处理
    DatabaseAccessor accessor = DatabaseAccessorFactory.getAccessor(conf);
    Pair<String, Long> pair = accessor.getNullQuery(conf, partitionColumn);
    if (pair != null) {
      String left = pair.getLeft();
      splits.add(new JdbcInputSplit(left, 0L, pair.getRight().longValue(), path));
    }
  }

  public void addSplit(Object lower, Object upper, long lenght, int index) {
    if (index == this.getPartitionNum() - 1) {
      this.getSplits().add(
          new JdbcInputSplit(getQuery(lower.toString(), null, conf), 0L, lenght, this.getPath()));
    } else if (index == 0) {
      this.getSplits().add(
          new JdbcInputSplit(getQuery(null, upper.toString(), conf), 0L, lenght, this.getPath()));
    } else {
      this.getSplits().add(
          new JdbcInputSplit(getQuery(lower.toString(), upper.toString(), conf), 0L, lenght,
              this.getPath()));
    }
  }

  @Override
  public String getQuery(String lowerBound, String upperBound, Configuration conf) {
    String sql = JdbcStorageConfigManager.getQueryToExecute(conf);
    if (StringUtils.isBlank(lowerBound) && StringUtils.isBlank(upperBound)) {
      return sql;
    }
    StringBuilder boundary = new StringBuilder();
    boundary.append("(");
    if (lowerBound != null) {
      boundary
          .append(String.format("%s >= %s ", partitionColumn, quotes() + lowerBound + quotes()));
    }
    if (upperBound != null) {
      if (lowerBound != null) {
        boundary.append(" and ");
      }
      boundary.append(String.format("%s < %s ", partitionColumn, quotes() + upperBound + quotes()));
    }
  /*if (lowerBound == null && upperBound != null) {
	  boundary.append(String.format(" or %s is null", partitionColumn));
	}*/
    boundary.append(")");
    return sql + (sql.toLowerCase().contains("where") ? " and " : " where ") + boundary.toString();
  }

  @Override
  public String quotes() {
    return "'";
  }

  public void setNullPartLength(long length) {
    if (this.getSplits().size() != 0) {
      this.getSplits().get(0).setLength(Long.valueOf(length));
    }
  }

  public static InputSplit[] initSingle(Configuration conf, Integer lenght) {
    InputSplit[] inputSplits = new InputSplit[1];
    inputSplits[0] = new JdbcInputSplit(JdbcStorageConfigManager.getQueryToExecute(conf), 0L,
        lenght, FileInputFormat.getInputPaths((JobConf) conf)[0]);
    return inputSplits;
  }

  @Override
  public String toString() {
    StringBuffer toString = new StringBuffer();
    toString.append("splitter:" + this.getClass().getSimpleName() + "##");
    toString.append(String
        .format("partColumn:%s,minbound:%s,maxbound:%s,partSize:%s##", this.partitionColumn,
            this.getLowerBound(), this.getUpperBound(), this.partitionNum));
    toString.append(String.format("splits:[%s]", StringUtils.join(this.getSplits(), "\t")));
    return toString.toString();
  }

  public void print() {
    System.out.println(
        ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    System.out.println("splitter:" + this.getClass().getSimpleName());
    System.out.println(String
        .format("partColumn:%s,minbound:%s,maxbound:%s,partSize:%s", this.partitionColumn,
            this.getLowerBound(), this.getUpperBound(), this.partitionNum));
    for (InputSplit split : this.splits) {
      System.out.println(split.toString());
    }
    System.out.println(
        "--------------------------------------------end----------------------------------------------");
  }

}
