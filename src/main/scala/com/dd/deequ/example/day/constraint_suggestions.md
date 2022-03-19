# Deequ Constraint suggestions


```scala
val df:DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
    val dfFirstDay = df.filter("dteday rlike '2011-01-'")
```
Important is to do correct filtering
- check it out with year only filtering

```scala
 val suggestionResult = ConstraintSuggestionRunner()
      .onData(dfFirstDay)
      .addConstraintRules(Rules.DEFAULT)
      .run()

suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
  suggestions.foreach { suggestion =>
    println(suggestion.codeForConstraint)
  }
}
```
Generated checks are ready to copypaste into your checks code
- but you need to be aware that the data used only some data subset
- because of it - you need to adjust yours checks - especially the **isContainedIn**
```
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
```

[Source code](ConstraintSuggestionDay.scala)