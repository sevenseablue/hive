/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.beeline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

public class TestCommands {

  @Test
  public void testLinesEndingWithComments() {
    BeeLine beeline = new BeeLine();
    Commands commands = new Commands(beeline);
    int[] escape = {-1};
    assertEquals("show tables;", commands.removeComments("show tables;",escape));
    assertEquals("show tables;", commands.removeComments("show tables; --comments",escape));
    assertEquals("show tables;", commands.removeComments("show tables; -------comments",escape));
    assertEquals("show tables;", commands.removeComments("show tables; -------comments;one;two;three;;;;",escape));
    assertEquals("show", commands.removeComments("show-- tables; -------comments",escape));
    assertEquals("show", commands.removeComments("show --tables; -------comments",escape));
    assertEquals("s", commands.removeComments("s--how --tables; -------comments",escape));
    assertEquals("", commands.removeComments("-- show tables; -------comments",escape));

    assertEquals("\"show tables\"", commands.removeComments("\"show tables\" --comments",escape));
    assertEquals("\"show --comments tables\"", commands.removeComments("\"show --comments tables\" --comments",escape));
    assertEquals("\"'show --comments' tables\"", commands.removeComments("\"'show --comments' tables\" --comments",escape));
    assertEquals("'show --comments tables'", commands.removeComments("'show --comments tables' --comments",escape));
    assertEquals("'\"show --comments tables\"'", commands.removeComments("'\"show --comments tables\"' --comments",escape));
  }

  /**
   * Test the commands directly call from beeline.
   * @throws IOException
   */
  @Test
  public void testBeelineCommands() throws IOException {
 // avoid System.exit() call in beeline which causes JVM to exit and fails the test
    System.setProperty(BeeLineOpts.PROPERTY_NAME_EXIT, "true");
    // Verify the command without ';' at the end also works fine
    BeeLine.mainWithInputRedirection(new String[] {"-u", "jdbc:hive2://", "-e", "select 3"}, null);
    BeeLine.mainWithInputRedirection(
        new String[] {"-u", "jdbc:hive2://", "-e", "create table t1(x int); show tables"}, null);
  }
}

