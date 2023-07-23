# Macrometa Collections-Streams Databricks Connector

In this example we want to show you how to read data from a Macrometa Stream and write data to a Macrometa Collection using the Macrometa Collection and Stream Databricks Connector.

## Requirements

- Databricks Runtime 11.3 LTS(with Apache Spark 3.3.0)
- Scala 2.12 or later
- Macrometa account with access to streams

## Usage

### Reading from a Macrometa Stream

1. Set up your source options:

```scala
val sourceOptions = Map(
  "regionUrl" -> regionUrl,
  "port" -> "6651",
  "apikey" -> apikey,
  "fabric" -> fabric,
  "tenant" -> tenant,
  "replication" -> replication,
  "stream" -> sourceStream,
  "subscriptionName" -> sourceSubscription
)
```

2. Create a spark session:
```scala
val spark = SparkSession.builder()
  .appName("MacrometaCollectionStreamingApp")
  .master("local[*]")
  .getOrCreate()
```
3. Read from the Macrometa stream:
```scala
  val inputStream = spark.readStream
  .format("com.macrometa.spark.stream.MacrometaTableProvider")
  .options(sourceOptions)
  .load()
````

### Writing to a Macrometa collection
1. Set up your target options:
```scala
val targetOptions = Map(
  "regionUrl" -> regionUrl,
  "apiKey" -> "apikey ",
  "fabric" -> fabric,
  "collection" -> "<YOUR_TARGET_COLLECTION>",
  "batchSize" -> "100"
)
```
2. Write to the Macrometa collection:
```scala
  val query = inputStream.writeStream
  .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    batchDF.write
      .format("com.macrometa.spark.collection.MacrometaTableProvider")
      .options(targetOptions)
      .mode(SaveMode.Append)
      .save()
  }
  .option("checkpointLocation", "checkpoint")
  .start()
````
3. Close SparkSession:
```scala
query.awaitTermination()
```
