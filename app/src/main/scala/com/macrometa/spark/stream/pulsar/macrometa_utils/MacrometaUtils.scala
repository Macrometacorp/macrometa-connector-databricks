/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.pulsar.macrometa_utils

import org.apache.pulsar.client.api.{PulsarClient, SubscriptionInitialPosition, SubscriptionType, Schema => PulsarSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.{Failure, Success, Try}

class MacrometaUtils {
  def createTopic(options: CaseInsensitiveStringMap): String = {
    val scalaOptions: Map[String, String] = options.asScala.toMap
    createTopic(scalaOptions)
  }

  def createTopic(options: Map[String, String]): String = {
    val commonPrefix = s"persistent://${options("tenant")}/c8${options("replication")}.${options("fabric")}/"

    val isCollectionStream = Try(options("isCollectionStream").toLowerCase.toBoolean) match {
      case Success(booleanValue) => booleanValue
      case Failure(_) => false // Default value to return when the conversion fails
    }

    val topic = if (isCollectionStream) {
      s"${commonPrefix}${options("stream")}"
    } else {
      s"${commonPrefix}c8${options("replication")}s.${options("stream")}"
    }
    topic
  }

  def inferSchema(
      client: PulsarClient,
      options: CaseInsensitiveStringMap
  ): StructType = {
    val consumer = client
      .newConsumer(PulsarSchema.BYTES)
      .topic(createTopic(options))
      .subscriptionName(options.get("subscriptionName"))
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionType(SubscriptionType.Shared)
      .subscribe()
    val messageOpt: Option[Array[Byte]] =
      Try(consumer.receive()).toOption.map(_.getValue)
    val schema = messageOpt match {
      case Some(messageBytes) =>
        val messageJson = new String(messageBytes)
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        val df = spark.read.json(Seq(messageJson).toDS)
        df.schema
      case None =>
        new StructType()
    }
    consumer.close()
    schema
  }

}
