/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.read.microbatch

import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import com.macrometa.spark.stream.read.MacrometaInputPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, FloatType, LongType, NullType, StringType, StructType}
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.pulsar.client.api.{Consumer, Message, PulsarClient, SubscriptionInitialPosition, SubscriptionType, Schema => PulsarSchema}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.unsafe.types.UTF8String

import java.nio.charset.StandardCharsets

class MacrometaMicroBatchPartitionReader(
    options: Map[String, String],
    schema: StructType,
    partition: MacrometaInputPartition
) extends PartitionReader[InternalRow] {

  private val objectMapper = new ObjectMapper()
  private var currentMessage: Option[Message[Array[Byte]]] = None

  val client: PulsarClient = MacrometaPulsarClientInstance
    .getInstance(
      federation = options.getOrElse("regionurl", ""),
      port = options.getOrElse("port", 6651.toString),
      apikey = options.getOrElse("apikey", "")
    )
    .getClient

  val topic: String = new MacrometaUtils().createTopic(options)
  val consumer: Consumer[Array[Byte]] = client
    .newConsumer[Array[Byte]](PulsarSchema.BYTES)
    .topic(topic)
    .subscriptionName(options.getOrElse("subscriptionName", "test-subs"))
    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
    .subscriptionType(SubscriptionType.Shared)
    .subscribe()

  override def next(): Boolean = {
    val message = consumer.receive()
    val messageIdString = message.getMessageId.toString
    val Array(ledgerId, entryId, partitionIndex, _) = messageIdString.split(":")
    val (parsedLedgerId, parsedEntryId, parsedPartitionIndex) = (
      ledgerId.toLong,
      entryId.toLong,
      partitionIndex.toInt
    )
    val messageIdWithoutPartitionInstance = new MessageIdImpl(parsedLedgerId, parsedEntryId, parsedPartitionIndex)
    if (
      message != null && messageIdWithoutPartitionInstance.compareTo(
        partition.end.messageId
      ) <= 0
    ) {
      val fieldNames = schema.fields.map(_.name)
      val messageString = new String(message.getValue, StandardCharsets.UTF_8)
      val jsonNode = objectMapper.readTree(messageString)
      val missingFields = fieldNames.filterNot(jsonNode.has)
      if (missingFields.isEmpty) {
        currentMessage = Some(message)
        consumer.acknowledge(message.getMessageId)
        true
      } else {
        consumer.acknowledge(message.getMessageId)
        next()
      }
    } else {
      currentMessage = None
      false
    }

  }

  override def get(): InternalRow = {
    currentMessage.map { message =>
      val messageString = new String(message.getValue, StandardCharsets.UTF_8)
      val jsonNode = objectMapper.readTree(messageString)
      val values = schema.fields.map { field =>
        val value = jsonNode.get(field.name)
        if (value == null || value.isNull) {
          null
        } else {
          jsonNodeToRow(value, field.dataType)
        }
      }

      InternalRow.fromSeq(values)
    }.orNull
  }

  override def close(): Unit = {
    consumer.close()
  }

  private def jsonNodeToRow(value: JsonNode, dataType: DataType): Any = {
    dataType match {
      case StringType => if (value == null) null else UTF8String.fromString(value.asText())
      case LongType => if (value == null) null else value.asLong()
      case DoubleType => if (value == null) null else value.asDouble()
      case FloatType => if (value == null) null else value.asDouble().toFloat
      case BooleanType => if (value == null) null else value.asBoolean()
      case NullType => null
      case ArrayType(elementType, _) =>
        if (value != null && value.isArray) {
          val arrayValues = (0 until value.size).map { i =>
            jsonNodeToRow(value.get(i), elementType)
          }.toArray
          new GenericArrayData(arrayValues)
        } else {
          null
        }
      case structType: StructType =>
        if (value != null && value.isObject) {
          val values = structType.fields.map { field =>
            val fieldValue = value.get(field.name)
            jsonNodeToRow(fieldValue, field.dataType)
          }
          // Convert the values to a Row directly
          InternalRow.fromSeq(values)
        } else {
          null // Return null for missing struct
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type: $dataType"
        )
    }
  }
}
