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

package ai.chronon.spark.join

import ai.chronon.aggregator.test.Column
import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.api.planner.RelevantLeftForJoinPart
import ai.chronon.spark._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.batch.ModularMonolith
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.DataFrameGen
import org.junit.Assert._

class EventsEventsSnapshotTest extends BaseJoinTest {

  it should "test events events snapshot" in {
    val viewsSchema = List(
      Column("user", api.StringType, 10),
      Column("item", api.StringType, 10),
      Column("time_spent_ms", api.LongType, 5000)
    )

    val viewsTable = s"$namespace.view_events"
    DataFrameGen.events(spark, viewsSchema, count = 100, partitions = 200).drop("ts").save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = yearAgo),
      table = viewsTable
    )

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(
        Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")
      ),
      metaData = Builders.MetaData(name = "unit_test.item_views", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )

    // left side
    val itemQueries = List(Column("item", api.StringType, 100))
    val itemQueriesTable = s"$namespace.item_queries"
    DataFrameGen
      .events(spark, itemQueries, 100, partitions = 100)
      .save(itemQueriesTable)

    val start = tableUtils.partitionSpec.minus(today, new Window(100, TimeUnit.DAYS))

    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = start), table = itemQueriesTable),
      joinParts = Seq(Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_2", namespace = namespace, team = "chronon")
    )

    // Run the ModularMonolith pipeline
    val dateRange = new DateRange()
      .setStartDate(start)
      .setEndDate(monthAgo)
    ModularMonolith.run(joinConf, dateRange)(tableUtils)

    // Read the computed output from the output table
    val computed = tableUtils.sql(s"SELECT * FROM ${joinConf.metaData.outputTable}")
    computed.show()

    val expected = tableUtils.sql(s"""
                                     |WITH
                                     |   queries AS (SELECT item, ts, ds from $itemQueriesTable where ds >= '$start' and ds <= '$monthAgo')
                                     | SELECT queries.item,
                                     |        queries.ts,
                                     |        queries.ds,
                                     |        AVG(IF(queries.ds > $viewsTable.ds, time_spent_ms, null)) as user_unit_test_item_views_time_spent_ms_average
                                     | FROM queries left outer join $viewsTable
                                     |  ON queries.item = $viewsTable.item
                                     | WHERE ($viewsTable.item IS NOT NULL) AND $viewsTable.ds >= '$yearAgo' AND $viewsTable.ds <= '$dayAndMonthBefore'
                                     | GROUP BY queries.item, queries.ts, queries.ds, from_unixtime(queries.ts/1000, 'yyyy-MM-dd')
                                     |""".stripMargin)
    expected.show()

    val diff = Comparison.sideBySide(computed, expected, List("item", "ts", "ds"))

    if (diff.count() > 0) {
      println(s"Diff count: ${diff.count()}")
      println("diff result rows")
      diff.show()
    }
    assertEquals(diff.count(), 0)
  }

  it should "reuse existing monolith join part partitions" in {
    val leftTable = s"$namespace.item_queries_monolith_part_reuse"
    val viewsTable = s"$namespace.view_events_monolith_part_reuse"
    val outputTable = s"$namespace.test_item_snapshot_features_monolith_part_reuse"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")

    val end = threeDaysAgo
    val rightDs = tableUtils.partitionSpec.before(end)
    val ts = spark
      .sql(s"SELECT CAST(unix_timestamp('$end', 'yyyy-MM-dd') * 1000 + 3600000 AS BIGINT)")
      .head()
      .getLong(0)

    spark
      .sql(s"SELECT 'item_a' AS item, $ts AS ts, '$end' AS ds")
      .save(leftTable)
    spark
      .sql(s"SELECT 'item_a' AS item, CAST(1 AS BIGINT) AS time_spent_ms, '$rightDs' AS ds")
      .save(viewsTable)

    val viewsSource = Builders.Source.events(
      query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = rightDs),
      table = viewsTable
    )
    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(viewsSource),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders.MetaData(name = "unit_test.item_views_monolith_part_reuse", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = end), table = leftTable),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_monolith_part_reuse",
                                   namespace = namespace,
                                   team = "chronon")
    )

    val partTable = RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
    spark.sql(s"DROP TABLE IF EXISTS $partTable")
    spark
      .sql(s"""
              |SELECT
              |  'item_a' AS item,
              |  CAST(999.0 AS DOUBLE) AS time_spent_ms_average,
              |  '$rightDs' AS ds
              |""".stripMargin)
      .save(partTable)

    new ai.chronon.spark.Join(joinConf, end, tableUtils).computeJoin(Some(1))

    val actual = tableUtils
      .sql(s"SELECT user_unit_test_item_views_monolith_part_reuse_time_spent_ms_average FROM $outputTable")
      .head()
      .getDouble(0)
    val partValue = tableUtils
      .sql(s"SELECT time_spent_ms_average FROM $partTable")
      .head()
      .getDouble(0)

    assertEquals(999.0, actual, 0.0)
    assertEquals(999.0, partValue, 0.0)
  }

  it should "compute missing monolith snapshot join part partitions from the left event partition" in {
    val leftTable = s"$namespace.item_queries_monolith_snapshot_part_compute"
    val viewsTable = s"$namespace.view_events_monolith_snapshot_part_compute"
    val outputTable = s"$namespace.test_item_snapshot_features_monolith_snapshot_part_compute"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")

    val end = threeDaysAgo
    val rightDs = tableUtils.partitionSpec.before(end)
    val ts = spark
      .sql(s"SELECT CAST(unix_timestamp('$end', 'yyyy-MM-dd') * 1000 + 3600000 AS BIGINT)")
      .head()
      .getLong(0)

    spark
      .sql(s"SELECT 'item_a' AS item, $ts AS ts, '$end' AS ds")
      .save(leftTable)
    spark
      .sql(s"SELECT 'item_a' AS item, CAST(7 AS BIGINT) AS time_spent_ms, '$rightDs' AS ds")
      .save(viewsTable)

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = rightDs),
        table = viewsTable
      )),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders.MetaData(name = "unit_test.item_views_monolith_snapshot_part_compute", namespace = namespace),
      accuracy = Accuracy.SNAPSHOT
    )
    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = end), table = leftTable),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_monolith_snapshot_part_compute",
                                   namespace = namespace,
                                   team = "chronon")
    )

    val partTable = RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
    spark.sql(s"DROP TABLE IF EXISTS $partTable")

    new ai.chronon.spark.Join(joinConf, end, tableUtils).computeJoin(Some(1))

    val actual = tableUtils
      .sql(s"SELECT user_unit_test_item_views_monolith_snapshot_part_compute_time_spent_ms_average FROM $outputTable")
      .head()
      .getDouble(0)
    val partPartitions = tableUtils.partitions(partTable)

    assertEquals(7.0, actual, 0.0)
    assertEquals(Seq(rightDs), partPartitions)
  }

  it should "compute missing monolith temporal join part partitions on the left event partition" in {
    val leftTable = s"$namespace.item_queries_monolith_temporal_part_compute"
    val viewsTable = s"$namespace.view_events_monolith_temporal_part_compute"
    val outputTable = s"$namespace.test_item_snapshot_features_monolith_temporal_part_compute"
    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")

    val end = threeDaysAgo
    val leftTs = spark
      .sql(s"SELECT CAST(unix_timestamp('$end', 'yyyy-MM-dd') * 1000 + 3600000 AS BIGINT)")
      .head()
      .getLong(0)
    val rightTs = spark
      .sql(s"SELECT CAST(unix_timestamp('$end', 'yyyy-MM-dd') * 1000 + 1800000 AS BIGINT)")
      .head()
      .getLong(0)

    spark
      .sql(s"SELECT 'item_a' AS item, $leftTs AS ts, '$end' AS ds")
      .save(leftTable)
    spark
      .sql(s"SELECT 'item_a' AS item, $rightTs AS ts, CAST(11 AS BIGINT) AS time_spent_ms, '$end' AS ds")
      .save(viewsTable)

    val viewsGroupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(
        query = Builders.Query(selects = Builders.Selects("time_spent_ms"), startPartition = end),
        table = viewsTable
      )),
      keyColumns = Seq("item"),
      aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
      metaData = Builders.MetaData(name = "unit_test.item_views_monolith_temporal_part_compute", namespace = namespace),
      accuracy = Accuracy.TEMPORAL
    )
    val joinPart = Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")
    val joinConf = Builders.Join(
      left = Builders.Source.events(Builders.Query(startPartition = end), table = leftTable),
      joinParts = Seq(joinPart),
      metaData = Builders.MetaData(name = "test.item_snapshot_features_monolith_temporal_part_compute",
                                   namespace = namespace,
                                   team = "chronon")
    )

    val partTable = RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
    spark.sql(s"DROP TABLE IF EXISTS $partTable")

    new ai.chronon.spark.Join(joinConf, end, tableUtils).computeJoin(Some(1))

    val actual = tableUtils
      .sql(s"SELECT user_unit_test_item_views_monolith_temporal_part_compute_time_spent_ms_average FROM $outputTable")
      .head()
      .getDouble(0)
    val partPartitions = tableUtils.partitions(partTable)

    assertEquals(11.0, actual, 0.0)
    assertEquals(Seq(end), partPartitions)
  }

  it should "reuse existing hourly monolith temporal join part partitions" in {
    import spark.implicits._
    val previousSmallMode = spark.conf.get("spark.chronon.backfill.small_mode.enabled", "true")
    spark.conf.set("spark.chronon.backfill.small_mode.enabled", "false")

    val hourlySpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 60 * 60 * 1000)
    val hourlyTableUtils = TableUtils(spark, hourlySpec)
    val leftTable = s"$namespace.item_queries_monolith_hourly_temporal_part_reuse"
    val viewsTable = s"$namespace.view_events_monolith_hourly_temporal_part_reuse"
    val outputTable = s"$namespace.test_item_snapshot_features_monolith_hourly_temporal_part_reuse"
    val existing = "2024-01-01-02-00"
    val later = "2024-01-01-03-00"
    val start = existing
    val end = later
    def tsAt(time: String): Long = TsUtils.datetimeToTs(s"2024-01-01 $time:00")
    val existingLeftTs = tsAt("02:20")
    val laterLeftTs = tsAt("03:20")
    val existingRightTs = tsAt("02:00")
    val laterRightTs = tsAt("03:00")
    def hourlyQuery(selects: Map[String, String] = null): Query = {
      val query = Builders.Query(selects = selects, startPartition = start, partitionColumn = hourlySpec.column)
      query.setPartitionFormat(hourlySpec.format)
      query.setPartitionInterval(WindowUtils.fromMillis(hourlySpec.spanMillis))
      query
    }

    spark.sql(s"DROP TABLE IF EXISTS $leftTable")
    spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
    spark.sql(s"DROP TABLE IF EXISTS $outputTable")

    try {
      Seq(("item_a", existingLeftTs, existing), ("item_a", laterLeftTs, later))
        .toDF("item", "ts", "ds")
        .save(leftTable)
      Seq(("item_a", existingRightTs, 7L, existing), ("item_a", laterRightTs, 11L, later))
        .toDF("item", "ts", "time_spent_ms", "ds")
        .save(viewsTable)

      val viewsGroupBy = Builders.GroupBy(
        sources = Seq(Builders.Source.events(
          query = hourlyQuery(Builders.Selects("time_spent_ms")),
          table = viewsTable
        )),
        keyColumns = Seq("item"),
        aggregations = Seq(Builders.Aggregation(operation = Operation.AVERAGE, inputColumn = "time_spent_ms")),
        metaData = Builders.MetaData(name = "unit_test.item_views_monolith_hourly_temporal_part_reuse",
                                     namespace = namespace),
        accuracy = Accuracy.TEMPORAL
      )
      val joinPart = Builders.JoinPart(groupBy = viewsGroupBy, prefix = "user")
      val joinConf = Builders.Join(
        left = Builders.Source.events(hourlyQuery(), table = leftTable),
        joinParts = Seq(joinPart),
        metaData = Builders.MetaData(name = "test.item_snapshot_features_monolith_hourly_temporal_part_reuse",
                                     namespace = namespace,
                                     team = "chronon")
      )

      val partTable = RelevantLeftForJoinPart.fullPartTableName(joinConf, joinPart)
      spark.sql(s"DROP TABLE IF EXISTS $partTable")
      Seq(("item_a", existingLeftTs, 999.0, existing))
        .toDF("item", "ts", "time_spent_ms_average", "ds")
        .save(partTable)

      new ai.chronon.spark.Join(joinConf, end, hourlyTableUtils).computeJoin(Some(1), Some(start))

      val actual = hourlyTableUtils
        .sql(s"""
                |SELECT ds, user_unit_test_item_views_monolith_hourly_temporal_part_reuse_time_spent_ms_average
                |FROM $outputTable
                |ORDER BY ds
                |""".stripMargin)
        .collect()
        .map(row => row.getString(0) -> row.getDouble(1))
        .toMap
      val partValues = hourlyTableUtils
        .sql(s"SELECT ds, time_spent_ms_average FROM $partTable")
        .collect()
        .map(row => row.getString(0) -> row.getDouble(1))
        .toMap

      assertEquals(999.0, actual(existing), 0.0)
      assertEquals(9.0, actual(later), 0.0)
      assertEquals(999.0, partValues(existing), 0.0)
      assertEquals(9.0, partValues(later), 0.0)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $leftTable")
      spark.sql(s"DROP TABLE IF EXISTS $viewsTable")
      spark.sql(s"DROP TABLE IF EXISTS $outputTable")
      spark.conf.set("spark.chronon.backfill.small_mode.enabled", previousSmallMode)
    }
  }
}
