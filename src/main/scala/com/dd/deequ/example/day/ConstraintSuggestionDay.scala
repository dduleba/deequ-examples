package com.dd.deequ.example.day

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.dd.deequ.example.utils.DeequExampleUtils.withSpark
import org.apache.spark.sql.DataFrame

object ConstraintSuggestionDay extends App {

  withSpark { spark =>
    val df: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .filter("dteday rlike '2011-01-'")
    df.show(numRows = 1000, truncate = false)

    val suggestionResult = ConstraintSuggestionRunner()
      .onData(df)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
      suggestions.foreach { suggestion =>
        println(s"Constraint suggestion for '$column':\t${suggestion.description}\n" +
          s"The corresponding scala code is ${suggestion.codeForConstraint}\n")
      }
      suggestions.foreach { suggestion =>
        println(suggestion.codeForConstraint)
      }
    }

    suggestionResult.constraintSuggestions.foreach { case (column, suggestions) =>
      suggestions.foreach { suggestion =>
        println(suggestion.codeForConstraint)
      }
    }
  }

}
