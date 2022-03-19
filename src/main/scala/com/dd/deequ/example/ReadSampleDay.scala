package com.dd.deequ.example

import com.dd.deequ.example.utils.DeequExampleUtils.withSpark


object ReadSampleDay extends App {
  withSpark { spark =>
    spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/day.csv").getPath)
      .show(false)
  }
}