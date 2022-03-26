package com.dd.deequ.example.day

import com.amazon.deequ.analyzers.Sum
import com.amazon.deequ.anomalydetection.BatchNormalStrategy
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.{AnomalyCheckConfig, VerificationResult, VerificationSuite}
import com.dd.deequ.example.utils.DeequExampleUtils.withSpark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, ShortType}
import org.apache.spark.sql.{DataFrame, functions}

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, ZoneId}


object AnomalyBatchNormalStrategy extends App {
  withSpark(func = spark => {
    val byAll = Window.partitionBy("yr").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val by1w = Window.partitionBy("yr").rowsBetween(Window.currentRow - 7, Window.currentRow)

    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .withColumn("short_cnt", col("cnt").cast(ShortType))
      .withColumn("double_temp", col("temp").cast(DoubleType))
      .withColumn("short_season", col("season").cast(ShortType))
      .withColumn("avgAll", functions.avg(col("short_cnt")).over(byAll))
      .withColumn("avg1w", functions.avg(col("short_cnt")).over(by1w))
      .withColumn("mean", col("short_cnt") / functions.avg(col("short_cnt")).over(by1w))

    val property = "java.io.tmpdir"
    val anomalyJsonPath = Paths.get(System.getProperty(property), "anomaly.json").toString
    println(s"anomaly json file path: ${anomalyJsonPath}")
    val metricsRepository = new FileSystemMetricsRepository(spark, path = anomalyJsonPath)

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Etc/UTC"))
    val dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("Etc/UTC"))

    val startDate = Instant.from(formatter.parse("2011-01-01 00:00:00"))
    val startTimestamp = startDate.toEpochMilli
    val endDate = Instant.from(formatter.parse("2011-01-31 00:00:00"))
    val days = ChronoUnit.DAYS.between(startDate, endDate)

    df.filter(col("dteday").between(startDate, endDate)).show(numRows = 1000, truncate = false)

    val dfResults = (0l to days).map(
      delta_date => {
        val date = startDate.plus(Duration.ofDays(delta_date))
        val dateStr = dayFormatter.format(date)
        val ts = date.toEpochMilli
        println(s"$dateStr: ${ts}")
        val resultKey = ResultKey(ts)

        val anomalyCheckErrorConfig = Some(AnomalyCheckConfig(
          level = CheckLevel.Error,
          description = s"anomaly check for $dateStr: ${ts}",
          withTagValues = Map.empty,
          afterDate = Some(ts - 7 * 24 * 60 * 60 * 1000),
          beforeDate = Some(ts - 1)
        ))

        val verificationResult = VerificationSuite()
          .onData(df.filter(s"dteday = '${dateStr}'"))
          .useRepository(metricsRepository)
          .saveOrAppendResult(resultKey)
          .addAnomalyCheck(
            BatchNormalStrategy(Some(3.0), Some(3.0)),
            Sum("short_cnt"),
            anomalyCheckErrorConfig
          )
          .run()
        VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
      }
    ).reduce(_ union _)
    metricsRepository
      .load()
      .forAnalyzers(Seq(Sum("short_cnt")))
      .getSuccessMetricsAsDataFrame(spark).orderBy("dataset_date")
      .show(1000, truncate = false)
    dfResults.show(1000, truncate = false)
  }
  )
}
