package com.dd.deequ.example.day

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.dd.deequ.example.utils.DeequExampleUtils.withSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, ShortType}

object VerifyWithErrorsDay extends App {
  withSpark { spark =>
    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .filter("dteday rlike '2012-'")
      .withColumn("short_cnt", col("cnt").cast(ShortType))
      .withColumn("double_temp", col("temp").cast(DoubleType))
      .withColumn("short_season", col("season").cast(ShortType))
    df.show(numRows = 1000, truncate = false)

    val verificationResult = VerificationSuite()
      .onData(df)
      .addCheck(Check(CheckLevel.Error, "unit testing Bike sharing day data")
        // not satisfied constraints
        .isUnique("temp")
        .hasDistinctness(Seq("temp"), _ == 1)
        .hasDistinctness(Seq("yr"), _ < 1 / 366, Some(s"Check that there is no big distinctness of yr ${1.0 / 366}"))
        .hasDistinctness(Seq("instant"), _ < 1, Some("Check for instant uniqueness"))
        .hasSize(_ == 365, Some("Expected size 365"))
        .hasSum("cnt", _ > 10000000, Some("Expected bike shares cnt > 10000000"))
        .hasSum("short_cnt", _ > 10000000, hint=Some("Expected bike shares cnt > 10000000"))
        .hasCorrelation("short_season","double_temp", _ > 0.5)
        .hasCorrelation("double_temp","short_cnt", _ < 0.5)
      ).run

    val dfResults = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    dfResults.show(false)
  }
}
