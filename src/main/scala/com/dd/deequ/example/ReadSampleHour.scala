package com.dd.deequ.example

import com.dd.deequ.example.utils.DeequExampleUtils.withSpark


object ReadSampleHour extends App {
  withSpark { spark =>
    spark.read.format("csv")
      .option("header", "true")
      .load(getClass.getResource("/hour.csv").getPath)
      .show(false)
  }
}
