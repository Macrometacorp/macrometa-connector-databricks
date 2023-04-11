package com.macrometa.spark.stream.pulsar.macrometa_utils

import org.apache.pulsar.client.api.{PulsarClient, SubscriptionType, Schema => PulsarSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.util.Try

class MacrometaUtils {
  def createTopic(options: CaseInsensitiveStringMap): String = {
    val topic = s"persistent://${options.get("tenant")}/c8${options.get("replication")}._system/c8${options.get("replication")}s.${options.get("stream")}"
    topic
  }

  def createTopic(options: Map[String, String]): String = {
    val topic = s"persistent://${options("tenant")}/c8${options("replication")}._system/c8${options("replication")}s.${options("stream")}"
    topic
  }

  def inferSchema(client: PulsarClient, options: CaseInsensitiveStringMap): StructType = {
    val consumer = client.newConsumer (PulsarSchema.BYTES).topic(createTopic(options)).subscriptionName(options.get("subscriptionName")).subscriptionType (SubscriptionType.Shared).subscribe()
    val messageOpt: Option[Array[Byte]] = Try (consumer.receive () ).toOption.map (_.getValue)
    val schema = messageOpt match {
    case Some (messageBytes) =>
    val messageJson = new String (messageBytes)
    val spark = SparkSession.builder ().getOrCreate ()
    import spark.implicits._
    val df = spark.read.json (Seq (messageJson).toDS)
    df.schema
    case None =>
    new StructType ()
  }
    consumer.close ()
    schema
  }

}
