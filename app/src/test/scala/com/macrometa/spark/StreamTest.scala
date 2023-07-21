/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.macrometa.spark

import org.apache.spark.sql.SparkSession

object StreamTest extends App {

  val regionUrl = "*.macrometa.io"
  val port = "6651"
  val fabric = "_system"
  val tenant = "<YOUR_TENANT>"
  val replication = "global"
  val sourceStream = "<SOURCE_SAMPLE_STREAM_NAME>"
  val apikey = "<YOUR_APIKEY>"
  val isCollectionStream = "false"
  val sourceSubscription = "test-subscription-123"

  val sourceOptions = Map(
    "regionUrl" -> regionUrl,
    "port" -> port,
    "apikey" -> apikey,
    "fabric" -> fabric,
    "tenant" -> tenant,
    "replication" -> replication,
    "stream" -> sourceStream,
    "isCollectionStream" -> isCollectionStream,
    "subscriptionName" -> sourceSubscription
  )

  val spark = SparkSession
    .builder()
    .appName("MacrometaStreamingApp")
    .master("local[*]")
    .getOrCreate()

  val inputStream = spark.readStream
    .format("com.macrometa.spark.stream.MacrometaTableProvider")
    .options(sourceOptions)
    .load()

  val targetStream = "<TARGET_SAMPLE_STREAM_NAME>"
  val targetOptions = Map(
    "regionUrl" -> regionUrl,
    "port" -> port,
    "apikey" -> apikey,
    "fabric" -> fabric,
    "tenant" -> tenant,
    "replication" -> replication,
    "stream" -> targetStream,
    "checkpointLocation" -> "checkpoint"
  )

  val query = inputStream
    .select("symbol", "ma")
    .withColumnRenamed("ma", "value")
    .writeStream
    .format("com.macrometa.spark.stream.MacrometaTableProvider")
    .options(targetOptions)
    .start()
  query.awaitTermination()
  query.stop()

}
