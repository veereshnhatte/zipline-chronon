/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.other

import ai.chronon.spark.Driver.OfflineSubcommand
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.rogach.scallop.ScallopConf
import org.scalatest.flatspec.AnyFlatSpec

class OfflineSubcommandTest extends AnyFlatSpec {

  class TestArgs(args: Array[String]) extends ScallopConf(args) with OfflineSubcommand {
    verify()

    override def subcommandName: String = "test"

    override def buildSparkSession(): SparkSession = super.buildSparkSession()

    override def isLocal: Boolean = true
  }

  it should "basic is parsed correctly" in {
    val confPath = "joins/team/example_join.v1"
    val args = new TestArgs(Seq("--conf-path", confPath).toArray)
    assertEquals(confPath, args.confPath())
    assertTrue(args.localTableMapping.isEmpty)
  }

  it should "local table mapping is parsed correctly" in {
    val confPath = "joins/team/example_join.v1"
    val endData = "2023-03-03"
    val argList = Seq("--local-table-mapping", "a=b", "c=d", "--conf-path", confPath, "--end-date", endData)
    val args = new TestArgs(argList.toArray)
    assertTrue(args.localTableMapping.nonEmpty)
    assertEquals("b", args.localTableMapping("a"))
    assertEquals("d", args.localTableMapping("c"))
    assertEquals(confPath, args.confPath())
    assertEquals(endData, args.endDate())
  }

  it should "parse output-location when provided" in {
    val outputLocation = "s3://bucket/warehouse/path"
    val args = new TestArgs(Seq("--output-location", outputLocation).toArray)
    assertEquals(Some(outputLocation), args.outputLocation.toOption)
  }

  it should "leave output-location unset when not provided" in {
    val args = new TestArgs(Seq("--conf-path", "joins/team/example_join.v1").toArray)
    assertTrue(args.outputLocation.isEmpty)
  }
}
