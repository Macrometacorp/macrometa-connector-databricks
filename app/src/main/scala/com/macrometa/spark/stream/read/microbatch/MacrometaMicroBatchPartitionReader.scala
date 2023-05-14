/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.read.microbatch

import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import com.macrometa.spark.stream.read.MacrometaInputPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{
  BooleanType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper
}
import org.apache.pulsar.client.api.{
  Consumer,
  Message,
  PulsarClient,
  SubscriptionType,
  Schema => PulsarSchema
}
import org.apache.spark.sql.Row

import java.nio.charset.StandardCharsets

class MacrometaMicroBatchPartitionReader(
    options: Map[String, String],
    schema: StructType,
    partition: MacrometaInputPartition
) extends PartitionReader[InternalRow] {

  private val rowEncoder = RowEncoder(schema).resolveAndBind()
  private val objectMapper = new ObjectMapper()
  private var currentMessage: Option[Message[Array[Byte]]] = None

  val client: PulsarClient = MacrometaPulsarClientInstance
    .getInstance(
      federation = options.getOrElse("regionurl", ""),
      port = options.getOrElse("port", 6651.toString),
      jwtToken = options.getOrElse("token", "")
    )
    .getClient

  val topic: String = new MacrometaUtils().createTopic(options)
  val consumer: Consumer[Array[Byte]] = client
    .newConsumer[Array[Byte]](PulsarSchema.BYTES)
    .topic(topic)
    .subscriptionName("test-subs")
    .subscriptionType(SubscriptionType.Shared)
    .subscribe()

  override def next(): Boolean = {
    val message = consumer.receive()
    if (
      message != null && message.getMessageId.compareTo(
        partition.end.messageId
      ) <= 0
    ) {
      currentMessage = Some(message)
      consumer.acknowledge(message.getMessageId)
      true
    } else {
      currentMessage = None
      false
    }

  }

  override def get(): InternalRow = {
    currentMessage.map { message =>
      val messageString = new String(message.getValue, StandardCharsets.UTF_8)
      val jsonNode = objectMapper.readTree(messageString)
      val row = jsonNodeToRow(jsonNode, schema)
      rowEncoder.createSerializer().apply(row)
    }.orNull
  }

  override def close(): Unit = {
    consumer.close()
  }

  private def jsonNodeToRow(jsonNode: JsonNode, schema: StructType): Row = {
    val values = schema.fields.map { field =>
      val value = jsonNode.get(field.name)
      if (value == null || value.isNull) {
        null
      } else {
        field.dataType match {
          case StringType  => value.asText()
          case IntegerType => value.asInt()
          case LongType    => value.asLong()
          case DoubleType  => value.asDouble()
          case BooleanType => value.asBoolean()
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported data type: ${field.dataType}"
            )
        }
      }
    }
    Row.fromSeq(values)
  }
}
