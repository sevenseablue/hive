/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.HadoopShims;

import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

import javax.swing.text.StyledEditorKit;

public class TestFileUtils {

  public static final Logger LOG = LoggerFactory.getLogger(TestFileUtils.class);

  private FileSystem getFs() {
    Configuration conf = new Configuration();
    conf.set("fs.viewfs.impl", org.apache.hadoop.fs.viewfs.ViewFileSystem.class.getName());
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    String HADOOP_HOME = System.getenv("HADOOP_HOME");
    System.out.println("HADOOP_HOME: "+HADOOP_HOME);
    conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/core-site.xml"));
    conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path(HADOOP_HOME+"/etc/hadoop/mountTable.xml"));
    FileSystem fs = null;
    try {
//      URI uri = new URI("viewfs://qunarcluster/user/datadev");
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      LOG.warn("error:", e);
    }
    return fs;
  }

  @Test
  public void isOwnerOfFileHierarchy() throws IOException {
    FileSystem fs = getFs();
    FileStatus fileStatus = fs.getFileStatus(new Path("viewfs://qunar22test/user/corphive/hive/warehouse/pp_hive.db/dw_pp_ods_device_active"));
    String userName = "corphive";
    FsAction fsAction = FsAction.READ;
    boolean res = FileUtils.isOwnerOfFileHierarchy(fs, fileStatus, userName, true, 0);
    System.out.println(res);
  }

  @Test
  public void isActionPermittedForFileHierarchy() throws Exception {
    FileSystem fs = getFs();
//    FileStatus fileStatus = fs.getFileStatus(new Path("viewfs://qunar22test/user/corphive/hive/warehouse/test.db/t2"));
    FileStatus fileStatus = fs.getFileStatus(new Path("viewfs://qunar22test/user/corphive/hive/warehouse/pp_hive.db/dw_pp_ods_device_active"));
    String userName = "pphive";
    FsAction fsAction = FsAction.WRITE;
    boolean permitted = FileUtils.isActionPermittedForFileHierarchy(getFs(), fileStatus, userName, fsAction);
    System.out.println(permitted);
    System.out.println("hello");
  }

  @Test
  public void isPathWithinSubtree_samePrefix() {
    Path path = new Path("/somedir1");
    Path subtree = new Path("/somedir");

    assertFalse(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_rootIsInside() {
    Path path = new Path("/foo");
    Path subtree = new Path("/foo");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_descendantInside() {
    Path path = new Path("/foo/bar");
    Path subtree = new Path("/foo");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void isPathWithinSubtree_relativeWalk() {
    Path path = new Path("foo/../../bar");
    Path subtree = new Path("../bar");
    assertTrue(FileUtils.isPathWithinSubtree(path, subtree));
  }

  @Test
  public void getParentRegardlessOfScheme_badCases() {
    Path path = new Path("proto://host1/foo/bar/baz");
    ArrayList<Path> candidates = new ArrayList<>();
    candidates.add(new Path("badproto://host1/foo"));
    candidates.add(new Path("proto://badhost1/foo"));
    candidates.add(new Path("proto://host1:71/foo/bar/baz"));
    candidates.add(new Path("proto://host1/badfoo"));
    candidates.add(new Path("/badfoo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertNull("none of these paths may match", res);
  }

  @Test
  public void getParentRegardlessOfScheme_priority() {
    Path path = new Path("proto://host1/foo/bar/baz");
    ArrayList<Path> candidates = new ArrayList<>();
    Path expectedPath;
    candidates.add(new Path("proto://host1/"));
    candidates.add(expectedPath = new Path("proto://host1/foo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertEquals(expectedPath, res);
  }

  @Test
  public void getParentRegardlessOfScheme_root() {
    Path path = new Path("proto://host1/foo");
    ArrayList<Path> candidates = new ArrayList<>();
    Path expectedPath;
    candidates.add(expectedPath = new Path("proto://host1/foo"));
    Path res = FileUtils.getParentRegardlessOfScheme(path, candidates);
    assertEquals(expectedPath, res);
  }

  @Test
  public void testGetJarFilesByPath() {
    HiveConf conf = new HiveConf(this.getClass());
    File tmpDir = Files.createTempDir();
    String jarFileName1 = tmpDir.getAbsolutePath() + File.separator + "a.jar";
    String jarFileName2 = tmpDir.getAbsolutePath() + File.separator + "b.jar";
    File jarFile1 = new File(jarFileName1);
    try {
      org.apache.commons.io.FileUtils.touch(jarFile1);
      Set<String> jars = FileUtils.getJarFilesByPath(tmpDir.getAbsolutePath(), conf);
      Assert.assertEquals(Sets.newHashSet("file://" + jarFileName1), jars);

      jars = FileUtils.getJarFilesByPath("/folder/not/exist", conf);
      Assert.assertTrue(jars.isEmpty());

      File jarFile2 = new File(jarFileName2);
      org.apache.commons.io.FileUtils.touch(jarFile2);
      String newPath = "file://" + jarFileName1 + "," + "file://" + jarFileName2 + ",/file/not/exist";
      jars = FileUtils.getJarFilesByPath(newPath, conf);
      Assert.assertEquals(Sets.newHashSet("file://" + jarFileName1, "file://" + jarFileName2), jars);
    } catch (IOException e) {
      LOG.error("failed to copy file to reloading folder", e);
      Assert.fail(e.getMessage());
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tmpDir);
    }
  }

  @Test
  public void testRelativePathToAbsolutePath() throws IOException {
    LocalFileSystem localFileSystem = new LocalFileSystem();
    Path actualPath = FileUtils.makeAbsolute(localFileSystem, new Path("relative/path"));
    Path expectedPath = new Path(localFileSystem.getWorkingDirectory(), "relative/path");
    assertEquals(expectedPath.toString(), actualPath.toString());

    Path absolutePath = new Path("/absolute/path");
    Path unchangedPath = FileUtils.makeAbsolute(localFileSystem, new Path("/absolute/path"));

    assertEquals(unchangedPath.toString(), absolutePath.toString());
  }

  @Test
  public void testIsPathWithinSubtree() throws IOException {
    Path splitPath = new Path("file:///user/hive/warehouse/src/data.txt");
    Path splitPathWithNoSchema = Path.getPathWithoutSchemeAndAuthority(splitPath);

    Set<Path> parents = new HashSet<>();
    FileUtils.populateParentPaths(parents, splitPath);
    FileUtils.populateParentPaths(parents, splitPathWithNoSchema);

    Path key = new Path("/user/hive/warehouse/src");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, true);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("/user/hive/warehouse/src_2");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, false);

    key = new Path("/user/hive/warehouse/src/data.txt");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, true);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("file:///user/hive/warehouse/src");
    verifyIsPathWithInSubTree(splitPath, key, true);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, true);

    key = new Path("file:///user/hive/warehouse/src_2");
    verifyIsPathWithInSubTree(splitPath, key, false);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, false);

    key = new Path("file:///user/hive/warehouse/src/data.txt");
    verifyIsPathWithInSubTree(splitPath, key, true);
    verifyIsPathWithInSubTree(splitPathWithNoSchema, key, false);
    verifyIfParentsContainPath(key, parents, true);
  }

  private void verifyIsPathWithInSubTree(Path splitPath, Path key, boolean expected) {
    boolean result = FileUtils.isPathWithinSubtree(splitPath, key);
    assertEquals("splitPath=" + splitPath + ", key=" + key, expected, result);
  }

  private void verifyIfParentsContainPath(Path key, Set<Path> parents, boolean expected) {
    boolean result = parents.contains(key);
    assertEquals("key=" + key, expected, result);
  }

  @Test
  public void testCopyWithDistcp() throws IOException {
    Path copySrc = new Path("copySrc");
    Path copyDst = new Path("copyDst");
    HiveConf conf = new HiveConf(TestFileUtils.class);
    conf.set(HiveConf.ConfVars.HIVE_WAREHOUSE_SUBDIR_INHERIT_PERMS.varname, "false");

    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(URI.create("hdfs:///"));

    ContentSummary mockContentSummary = mock(ContentSummary.class);
    when(mockContentSummary.getFileCount()).thenReturn(Long.MAX_VALUE);
    when(mockContentSummary.getLength()).thenReturn(Long.MAX_VALUE);
    when(mockFs.getContentSummary(any(Path.class))).thenReturn(mockContentSummary);

    HadoopShims shims = mock(HadoopShims.class);
    when(shims.runDistCp(copySrc, copyDst, conf)).thenReturn(true);

    Assert.assertTrue(FileUtils.copy(mockFs, copySrc, mockFs, copyDst, false, false, conf, shims));
    verify(shims).runDistCp(copySrc, copyDst, conf);
  }

  public static void main(String[] args) throws Exception {

    new TestFileUtils().isActionPermittedForFileHierarchy();
  }
}
