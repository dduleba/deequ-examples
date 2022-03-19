# Deequ Verify suggestion

## Verify last day with constraints from constraints suggestion

[Source code of verify last month data with constraints from constraints suggestion](VerifySuggestedConstraintsOnLastMonthDay.scala)

Output
```
...
We found errors in the data:

ComplianceConstraint(Compliance(yr contained in 0,`yr` IS NULL OR `yr` IN ('0'),None)): Value: 0.0 does not meet the constraint requirement!
ComplianceConstraint(Compliance(season contained in 1,`season` IS NULL OR `season` IN ('1'),None)): Value: 0.3548387096774194 does not meet the constraint requirement!
ComplianceConstraint(Compliance(mnth contained in 1,`mnth` IS NULL OR `mnth` IN ('1'),None)): Value: 0.0 does not meet the constraint requirement!
ComplianceConstraint(Compliance(yr contained in 0,`yr` IS NULL OR `yr` IN ('0'),None)): Value: 0.0 does not meet the constraint requirement!
```

## Verify data with adjusted constraints to reflect data changes during time

```shell
$ diff VerifySuggestedConstraintsOnLastMonthDay.scala VerifyCorrectedConstraintsOnLastMonthDay.scala
9c9
< object VerifySuggestedConstraintsOnLastMonthDay extends App {
---
> object VerifyCorrectedConstraintsOnLastMonthDay extends App {
24d23
<           .isContainedIn("yr", Array("0"))
48c47
<           .isContainedIn("season", Array("1"))
---
>           .isContainedIn("season", Array("1", "2", "3", "4"))
66c65
<           .isContainedIn("weekday", Array("0", "1", "6", "3", "2", "5", "4"), _ >= 0.99, Some("It should be above 0.99!"))
---
>           .isContainedIn("weekday", Array("0"), _ >= 0.1, Some("It should be above 0.99!"))
73c72
<           .isContainedIn("mnth", Array("1"))
---
>           .isContainedIn("mnth", (1 to 12).map(_.toString).toArray)
77d75
<           .isContainedIn("yr", Array("0"))
```

Output:
```
The data passed the test, everything is fine!
```

[Source of corrected constraints](VerifyCorrectedConstraintsOnLastMonthDay.scala)

## Examples with hint in error on last year

To some constraints we need proper data types in column in this example we add some columns with additional data type casting

```scala
val df: DataFrame = spark.read.format("csv")
  .option("header", "true")
  .load(getClass.getResource("/day.csv").getPath)
  .filter("dteday rlike '2012-'")
  .withColumn("short_cnt", col("cnt").cast(ShortType))
  .withColumn("double_temp", col("temp").cast(DoubleType))
  .withColumn("short_season", col("season").cast(ShortType))
```

We can add hint parameter to display some extra information in case of constraint failure
```scala
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
```

Convert results to dataframe
```scala
    val dfResults = VerificationResult.checkResultsAsDataFrame(spark, verificationResult)
    dfResults.show(false)
```

Output:
```
+----------------------------------+-----------+------------+-----------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|check                             |check_level|check_status|constraint                                                       |constraint_status|constraint_message                                                                                                                                                                     |
+----------------------------------+-----------+------------+-----------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|unit testing Bike sharing day data|Error      |Error       |UniquenessConstraint(Uniqueness(List(temp),None))                |Failure          |Value: 0.6311475409836066 does not meet the constraint requirement!                                                                                                                    |
|unit testing Bike sharing day data|Error      |Error       |DistinctnessConstraint(Distinctness(List(temp),None))            |Failure          |Value: 0.8005464480874317 does not meet the constraint requirement!                                                                                                                    |
|unit testing Bike sharing day data|Error      |Error       |DistinctnessConstraint(Distinctness(List(yr),None))              |Failure          |Value: 0.00273224043715847 does not meet the constraint requirement! Check that there is no big distinctness of yr 0.00273224043715847                                                 |
|unit testing Bike sharing day data|Error      |Error       |DistinctnessConstraint(Distinctness(List(instant),None))         |Failure          |Value: 1.0 does not meet the constraint requirement! Check for instant uniqueness                                                                                                      |
|unit testing Bike sharing day data|Error      |Error       |SizeConstraint(Size(None))                                       |Failure          |Value: 366 does not meet the constraint requirement! Expected size 365                                                                                                                 |
|unit testing Bike sharing day data|Error      |Error       |SumConstraint(Sum(cnt,None))                                     |Failure          |Expected type of column cnt to be one of (LongType,IntegerType,DoubleType,org.apache.spark.sql.types.DecimalType$@66716959,ByteType,FloatType,ShortType), but found StringType instead!|
|unit testing Bike sharing day data|Error      |Error       |SumConstraint(Sum(short_cnt,None))                               |Failure          |Value: 2049576.0 does not meet the constraint requirement! Expected bike shares cnt > 10000000                                                                                         |
|unit testing Bike sharing day data|Error      |Error       |CorrelationConstraint(Correlation(short_season,double_temp,None))|Failure          |Value: 0.29387642123889274 does not meet the constraint requirement!                                                                                                                   |
|unit testing Bike sharing day data|Error      |Error       |CorrelationConstraint(Correlation(double_temp,short_cnt,None))   |Failure          |Value: 0.7137931988838034 does not meet the constraint requirement!                                                                                                                    |
+----------------------------------+-----------+------------+-----------------------------------------------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
[Source with more examples](VerifyWithErrorsDay.scala)
