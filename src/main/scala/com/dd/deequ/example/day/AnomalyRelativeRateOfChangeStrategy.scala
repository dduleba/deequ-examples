package com.dd.deequ.example.day

import com.amazon.deequ.analyzers.Sum
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import com.amazon.deequ.checks.CheckLevel
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.{AnomalyCheckConfig, VerificationResult, VerificationSuite}
import com.dd.deequ.example.utils.DeequExampleUtils.withSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, ShortType}

import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit


object AnomalyRelativeRateOfChangeStrategy extends App {

  withSpark(func = spark => {
    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .withColumn("short_cnt", col("cnt").cast(ShortType))
      .withColumn("double_temp", col("temp").cast(DoubleType))
      .withColumn("short_season", col("season").cast(ShortType))
    df.show(numRows = 20, truncate = false)

    val metricsRepository = new InMemoryMetricsRepository()

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Etc/UTC"))
    val dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("Etc/UTC"))

    val startDate = Instant.from(formatter.parse("2011-01-01 00:00:00"))
    val startTimestamp = startDate.toEpochMilli
    println(startTimestamp)

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
          beforeDate = Some(ts - 1)
        ))

        val verificationResult = VerificationSuite()
          .onData(df.filter(s"dteday = '${dateStr}'"))
          .useRepository(metricsRepository)
          .saveOrAppendResult(resultKey)
          .addAnomalyCheck(
            RelativeRateOfChangeStrategy(
              maxRateIncrease = Some(1.2),
              maxRateDecrease = Some(0.8),
              order = 1),
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
