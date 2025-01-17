# Macrometa Stream Databricks Connector

The Macrometa Stream Databricks Connector allows you to integrate [Apache Spark](https://spark.apache.org/) with [Macrometa](https://www.macrometa.com/docs/streams/) streams, making it easy to process and analyze real-time data using Spark's powerful capabilities in a Databricks environment.

## Requirements

- Databricks Runtime 11.3 LTS(with Apache Spark 3.3.0)
- Scala 2.12 or later
- Macrometa account with access to streams

## Usage

### Reading from a Macrometa Stream

1. Set up your source options:

```scala
val sourceOptions = Map(
  "regionUrl" -> "<REGION_URL>",
  "port" -> "<PORT>",
  "apikey" -> "<APIKEY>",
  "fabric" -> "<FABRIC>",
  "tenant" -> "<TENANT>",
  "replication" -> "<REPLICATION>",
  "stream" -> "<SOURCE_STREAM>",
  "isCollectionStream" -> "<true or false>", // Indicates if this is a collection stream (true) or not (false), represented as a string value
  "subscriptionName" -> "<SOURCE_SUBSCRIPTION>"
)
```

2. Create a spark session:
```scala
val spark = SparkSession.builder()
  .appName("MacrometaStreamingApp")
  .master("local[*]")
  .getOrCreate()
```
3. Read from the Macrometa stream:
   1. Auto infer schema:
    ```scala
    val inputStream = spark.readStream
        .format("com.macrometa.spark.stream.MacrometaTableProvider")
        .options(sourceOptions)
        .load()
    ```

    2. User defined schema:
    ```scala
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val inputStream = spark.readStream
        .format("com.macrometa.spark.stream.MacrometaTableProvider")
        .options(sourceOptions)
        .schema(userSchema)
        .load()
    ```

### Writing to a Macrometa Stream
1. Set up your target options:
```scala
val targetOptions = Map(
  "regionUrl" -> "<REGION_URL>",
  "port" -> "<PORT>",
  "apikey" -> "<APIKEY>",
  "fabric" -> "<FABRIC>",
  "tenant" -> "<TENANT>",
  "replication" -> "<REPLICATION>",
  "stream" -> "<TARGET_STREAM>",
  "checkpointLocation" -> "<CHECKPOINT_LOCATION>"
)
```
2. Write to the Macrometa stream (Assuming the stream you are reading data from has the property 'symbol', 'ma'. Replace with your own schema:
```scala
  val query = inputStream.select("name","age")
  .withColumnRenamed("name", "userName")
  .writeStream
  .format("com.macrometa.spark.stream.MacrometaTableProvider")
  .options(targetOptions)
  .start()
````
3. Wait for termination:
```scala
query.awaitTermination()
```