# Anomaly detection

[Anomaly detection with RelativeRateOfChangeStrategy example](AnomalyRelativeRateOfChangeStrategy.scala)
```scala
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
```
[Anomaly detection with BatchNormalStrategy example](AnomalyBatchNormalStrategy.scala)
```scala
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
```

## ResultsKey timestamp
In the deequ example we can see with usage of **InMemoryMetricsRepository** is simple and usfeul
```scala
val yesterdaysKey = ResultKey(System.currentTimeMillis() - 24 * 60 * 60 * 1000)
val todaysKey = ResultKey(System.currentTimeMillis())
```

But in real life scenarios those results should be saved for further analysis with **FileSystemMetricsRepository** 
* In such cases we need to think about errors handling and keeping our anomaly checks source clean from errors
* because of that I recommend to use timestamp valid for your process data ingestion not for execution time

```scala
val property = "java.io.tmpdir"
val anomalyJsonPath = Paths.get(System.getProperty(property), "anomaly.json").toString
println(s"anomaly json file path: ${anomalyJsonPath}")
val metricsRepository = new FileSystemMetricsRepository(spark, path = anomalyJsonPath)
```

e.g. If you testing data for date _2011-01-01_ 
* the most important is data date analyzed not when it was actually executed

```scala
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Etc/UTC"))
val startDate = Instant.from(formatter.parse("2011-01-01 00:00:00"))
val ts = startDate.toEpochMilli
```

**anomaly.json** eventually will contain data day _2011-01-01_

```json
[                                        
  {                                      
    "resultKey": {                  
      "dataSetDate": 1293840000000, 
      "tags": {}                         
    },                                   
    "analyzerContext": {                 
      "metricMap": [                     
        {                           
          "analyzer": {             
            "analyzerName": "Sum",       
            "column": "short_cnt"        
          },                             
          "metric": {               
            "metricName": "DoubleMetric",
            "entity": "Column",  
            "instance": "short_cnt",     
            "name": "Sum",               
            "value": 985.0               
          }                         
        }                           
      ]                                  
    }                                    
  }
]
```
