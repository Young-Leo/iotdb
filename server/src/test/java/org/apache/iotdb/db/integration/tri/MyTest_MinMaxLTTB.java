/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.integration.tri;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

import static org.junit.Assert.fail;

public class MyTest_MinMaxLTTB {

  /*
   * Sql format: SELECT min_value(s0), max_value(s0) ROM root.xx group by ([tqs,tqe),IntervalLength).
   * Requirements:
   * (1) Don't change the sequence of the above two aggregates
   * (2) Assume each chunk has only one page.
   * (3) Assume all chunks are sequential and no deletes.
   * (4) Assume plain encoding, UNCOMPRESSED, Long or Double data type, no compaction
   */
  private static final String TIMESTAMP_STR = "Time";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        // IoTDB int data type does not support plain encoding, so use long data type
      };

  private final String d0s0 = "root.vehicle.d0.s0";

  private static final String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0)" + " VALUES(%d,%f)";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Before
  public void setUp() throws Exception {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("PLAIN");
    config.setTimestampPrecision("ms");
    config.setCompactionStrategy(CompactionStrategy.NO_COMPACTION);

    config.setEnableTri("MinMaxLTTB");

    // 但是如果走的是unpackOneChunkMetaData(firstChunkMetadata)就没问题，
    // 因为它直接用chunk元数据去构造pageReader，
    // 但是如果走的是传统聚合类型->seriesAggregateReader->seriesReader->hasNextOverlappedPage里
    // cachedBatchData = BatchDataFactory.createBatchData(dataType, orderUtils.getAscending(), true)
    // 这个路径就错了，把聚合类型赋给batchData了。所以这个LocalGroupByExecutor bug得在有overlap数据的时候才能复现
    // （那刚好我本文数据都不会有Overlap，可以用LocalGroupByExecutor来得到正确结果）
    config.setEnableCPV(false);
    TSFileDescriptor.getInstance().getConfig().setEnableMinMaxLSM(false);
    TSFileDescriptor.getInstance().getConfig().setUseStatistics(false);

    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test1() throws Exception {
    prepareData1();
    String res = "0,1[20],15[2],8[25],8[25],3[54],3[54],null,null,";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          // 注意从1开始编号，所以第一列是无意义时间戳
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData1() {
    // data:
    // https://user-images.githubusercontent.com/33376433/151985070-73158010-8ba0-409d-a1c1-df69bad1aaee.png
    // only first chunk
    // no overlap, no delete
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 15.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 1.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 25, 8.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 54, 3.0));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 120, 8.0));
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test2() throws Exception {
    prepareData2();
    String res =
        "-1.2079272[0],1.101946[200],-1.014322[700],0.809559[1500],-0.785419[1600],-0.0211206[2100],";
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([100,2100),250ms)");
      // rps=4,nout=6,minmaxInterval=floor((tn-t2)/((nout-2)*rps/2))=250ms
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          // 注意从1开始编号，所以第一列是无意义时间戳
          String ans = resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
      //      System.out.println(((IoTDBStatement) statement).executeFinish());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData2() {
    // data:
    // no overlap, no delete
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      int[] t =
          new int[] {
            0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500,
            1600, 1700, 1800, 1900, 2000, 2100
          };
      double[] v =
          new double[] {
            -1.2079272,
            -0.01120245,
            1.1019456,
            -0.52320362,
            -0.35970289,
            0.1453591,
            -0.45947892,
            -1.0143219,
            0.81760821,
            0.5325646,
            -0.29532424,
            -0.1469335,
            -0.12252526,
            -0.67607713,
            -0.16967308,
            0.8095585,
            -0.78541944,
            0.03221141,
            0.31586886,
            -0.41353356,
            -0.21019539,
            -0.0211206
          };
      for (int i = 0; i < t.length; i++) {
        statement.execute(String.format(Locale.ENGLISH, insertTemplate, t[i], v[i]));
      }
      statement.execute("FLUSH");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3() {
    prepareData3();

    //    String[] res = new String[]{"0,1[10],10[2]", "25,2[40],8[30]", "50,4[72],20[62]",
    // "75,1[90],11[80]"};
    String res = "0,1[10],10[2],2[40],8[30],4[72],20[62],1[90],11[80],";
    // 0,BPv[t]ofBucket1,TPv[t]ofBucket1,BPv[t]ofBucket2,TPv[t]ofBucket2,...
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test3_2() {
    prepareData3_2();

    //    String[] res = new String[]{"0,1[10],10[2]", "25,null,null", "50,4[72],20[62]",
    // "75,1[90],11[80]"};
    String res = "0,1[10],10[2],null,null,4[72],20[62],1[90],11[80],";
    // 0,BPv[t]ofBucket1,TPv[t]ofBucket1,BPv[t]ofBucket2,TPv[t]ofBucket2,...
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0), max_value(s0)"
                  + " FROM root.vehicle.d0 group by ([0,100),25ms)"); // don't change the
      // sequence!!!

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int i = 0;
        while (resultSet.next()) {
          String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(2);
          System.out.println(ans);
          Assert.assertEquals(res, ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareData3_2() {
    // data:
    // https://user-images.githubusercontent.com/33376433/152003603-6b4e7494-00ff-47e4-bf6e-cab3c8600ce2.png
    // remove chunk 2 so that bucket 2 is empty
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 1, 5));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 2, 10));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 10, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 20, 5));
      statement.execute("FLUSH");

      //      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 22, 4));
      //      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 30, 8));
      //      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 40, 2));
      //      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 55, 5));
      //      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 60, 15));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 62, 20));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 65, 8));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 70, 18));
      statement.execute("FLUSH");

      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 72, 4));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 80, 11));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 90, 1));
      statement.execute(String.format(Locale.ENGLISH, insertTemplate, 105, 7));
      statement.execute("FLUSH");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
