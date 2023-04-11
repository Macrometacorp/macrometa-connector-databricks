/*
 * This Scala source file was generated by the Gradle 'init' task.
 */
package com.macrometa.spark

import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{SaveMode, SparkSession}

object CollectionTest extends App {

    val apikey = "apikey "
    val federation = "*.macrometa.io"
    val fabric = "_system"
    val sourceCollection = "<YOUR_SOURCE_COLLECTION>"
    val batchSize = 10
    val query = s"FOR doc IN $sourceCollection RETURN doc"
    val targetCollection = "<YOUR_TARGET_COLLECTION>"
    val primaryKey = "number"


    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val sourceOptions = Map(
        "federation" -> federation,
        "apiKey" ->  apikey,
        "fabric" ->  fabric,
        "collection" -> sourceCollection,
        "batchSize" -> batchSize.toString,
        "query"-> query
    )

    val inputDF = spark.read
      .format("com.macrometa.spark.collection.MacrometaTableProvider")
      .options(sourceOptions)
      .load()

    inputDF.show()

    val modifiedDF = inputDF.select("value").withColumnRenamed("value", "number").
      withColumn("randomNumber", rand())

    val targetOptions = Map(
        "federation" -> federation,
        "apiKey" -> apikey,
        "fabric" -> fabric,
        "collection" -> targetCollection,
        "batchSize" -> batchSize.toString,
        "primaryKey" -> primaryKey
    )

    modifiedDF.write.format("com.macrometa.spark.collection.MacrometaTableProvider")
      .options(targetOptions)
      .mode(SaveMode.Append).save()

    spark.close()

}
