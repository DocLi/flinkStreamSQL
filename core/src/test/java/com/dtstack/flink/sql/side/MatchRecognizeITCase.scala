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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.side

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test
import org.apache.flink.test.util.AbstractTestBase
import scala.collection.mutable

class MatchRecognizeITCase extends AbstractTestBase {

  @Test
  def testSimpleCEP(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bid, T.cid
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    `A"`.id AS aid,
         |    \u006C.id AS bid,
         |    C.id AS cid
         |  PATTERN (`A"` \u006C C)
         |  DEFINE
         |    `A"` AS name = 'a',
         |    \u006C AS name = 'b',
         |    C AS name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("6,7,8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleCEPWithNulls(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String, String)]
    data.+=((1, "a", null))
    data.+=((2, "b", null))
    data.+=((3, "c", null))
    data.+=((4, "d", null))
    data.+=((5, null, null))
    data.+=((6, "a", null))
    data.+=((7, "b", null))
    data.+=((8, "c", null))
    data.+=((9, null, null))

    val t = env.fromCollection(data).toTable(tEnv, 'id, 'name, 'nullField, 'proctime.proctime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT T.aid, T.bNull, T.cid, T.aNull
         |FROM MyTable
         |MATCH_RECOGNIZE (
         |  ORDER BY proctime
         |  MEASURES
         |    A.id AS aid,
         |    A.nullField AS aNull,
         |    LAST(B.nullField) AS bNull,
         |    C.id AS cid
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS name = 'a' AND nullField IS NULL,
         |    B AS name = 'b' AND LAST(A.nullField) IS NULL,
         |    C AS name = 'c'
         |) AS T
         |""".stripMargin

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("1,null,3,null", "6,null,8,null")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
class ToMillis extends ScalarFunction {
  def eval(t: Timestamp): Long = {
    t.toInstant.toEpochMilli + TimeZone.getDefault.getOffset(t.toInstant.toEpochMilli)
  }
}
