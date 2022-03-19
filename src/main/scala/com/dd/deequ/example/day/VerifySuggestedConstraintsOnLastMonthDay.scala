package com.dd.deequ.example.day

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import com.dd.deequ.example.utils.DeequExampleUtils.withSpark
import org.apache.spark.sql.DataFrame

object VerifySuggestedConstraintsOnLastMonthDay extends App {

  withSpark { spark =>
    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .filter("dteday rlike '2012-12-'")
    df.show(numRows = 1000, truncate = false)

    val verificationResult = VerificationSuite()
      .onData(df)
      .addCheck(
        Check(CheckLevel.Error, "unit testing Bike sharing day data")
          .isComplete("yr")
          .hasDataType("yr", ConstrainableDataTypes.Integral)
          .isContainedIn("yr", Array("0"))
          .isNonNegative("yr")
          .isComplete("workingday")
          .hasDataType("workingday", ConstrainableDataTypes.Integral)
          .isContainedIn("workingday", Array("1", "0"))
          .isNonNegative("workingday")
          .isComplete("windspeed")
          .hasDataType("windspeed", ConstrainableDataTypes.Fractional)
          .isNonNegative("windspeed")
          .isComplete("registered")
          .hasDataType("registered", ConstrainableDataTypes.Integral)
          .isNonNegative("registered")
          .isComplete("atemp")
          .hasDataType("atemp", ConstrainableDataTypes.Fractional)
          .isNonNegative("atemp")
          .isComplete("weathersit")
          .hasDataType("weathersit", ConstrainableDataTypes.Integral)
          .isContainedIn("weathersit", Array("1", "2"), _ >= 0.9, Some("It should be above 0.9!"))
          .isNonNegative("weathersit")
          .isComplete("hum")
          .hasDataType("hum", ConstrainableDataTypes.Fractional)
          .isNonNegative("hum")
          .isComplete("season")
          .hasDataType("season", ConstrainableDataTypes.Integral)
          .isContainedIn("season", Array("1"))
          .isNonNegative("season")
          .isComplete("casual")
          .hasDataType("casual", ConstrainableDataTypes.Integral)
          .isNonNegative("casual")
          .isComplete("instant")
          .hasDataType("instant", ConstrainableDataTypes.Integral)
          .isNonNegative("instant")
          .isComplete("temp")
          .hasDataType("temp", ConstrainableDataTypes.Fractional)
          .isNonNegative("temp")
          .isComplete("holiday")
          .hasDataType("holiday", ConstrainableDataTypes.Integral)
          .isNonNegative("holiday")
          .isComplete("dteday")
          .isComplete("weekday")
          .hasDataType("weekday", ConstrainableDataTypes.Integral)
          .isContainedIn("weekday", Array("0", "1", "6", "3", "2", "5", "4"))
          .isContainedIn("weekday", Array("0", "1", "6", "3", "2", "5", "4"), _ >= 0.99, Some("It should be above 0.99!"))
          .isNonNegative("weekday")
          .isComplete("cnt")
          .hasDataType("cnt", ConstrainableDataTypes.Integral)
          .isNonNegative("cnt")
          .isComplete("mnth")
          .hasDataType("mnth", ConstrainableDataTypes.Integral)
          .isContainedIn("mnth", Array("1"))
          .isNonNegative("mnth")
          .isComplete("yr")
          .hasDataType("yr", ConstrainableDataTypes.Integral)
          .isContainedIn("yr", Array("0"))
          .isNonNegative("yr")
      )
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter {
          _.status != ConstraintStatus.Success
        }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }
  }
}
