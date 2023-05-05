/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.read.microbatch

import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import com.macrometa.spark.stream.read.MacrometaInputPartition
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.pulsar.client.api.{Consumer, MessageId, PulsarClient, SubscriptionType, Schema => PulsarSchema}
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

class MacrometaMicroBatchStream(options: CaseInsensitiveStringMap, schema: StructType) extends MicroBatchStream {
  val client: PulsarClient = MacrometaPulsarClientInstance.getInstance(federation = options.get("regionUrl"),
    port = options.getOrDefault("port",6651.toString), jwtToken = options.get("token")).getClient
  val topic: String = new MacrometaUtils().createTopic(options)
  val consumer: Consumer[Array[Byte]] = client.newConsumer[Array[Byte]](PulsarSchema.BYTES).topic(topic).
    subscriptionName(options.get("subscriptionName")).subscriptionType(SubscriptionType.Shared).subscribe()
  @volatile private var lastAckedMessageId: Option[MessageId] = None

  private def toMessageIdImpl(messageId: MessageId): MessageIdImpl = {
    val messageIdString = messageId.toString
    val Array(ledgerId, entryId, partitionIndex) = messageIdString.split(":").map(_.toLong)
    new MessageIdImpl(ledgerId, entryId, partitionIndex.toInt)
  }

  override def latestOffset(): Offset = {

    val messageId = consumer.getLastMessageId
    val messageIdImpl = toMessageIdImpl(messageId)
    MacrometaOffset(messageIdImpl)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[MacrometaOffset]
    val endOffset = end.asInstanceOf[MacrometaOffset]
    Array(MacrometaInputPartition(startOffset, endOffset))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val optionsMap = options.asScala.toMap
    new MacrometaMicroBatchPartitionReaderFactory(optionsMap, schema)
  }

  override def initialOffset(): Offset = {
    val earliestMessageId = MessageId.earliest
    val messageIdImpl = toMessageIdImpl(earliestMessageId)
    MacrometaOffset(messageIdImpl)
  }

  override def deserializeOffset(json: String): Offset = MacrometaOffset.fromJson(json)

  override def commit(end: Offset): Unit = {
    val endOffset = end.asInstanceOf[MacrometaOffset]
    if (lastAckedMessageId.isEmpty || !lastAckedMessageId.get.equals(endOffset.messageId)) {
      consumer.acknowledge(endOffset.messageId)
      lastAckedMessageId = Some(endOffset.messageId)
    }
  }

  override def stop(): Unit = {
    consumer.close()
  }
}

case class MacrometaOffset(messageId: MessageIdImpl) extends Offset {
  override def json(): String = {
    messageId.toString
    val (ledgerId, entryId, partitionIndex) = (
      messageId.getLedgerId,
      messageId.getEntryId,
      messageId.getPartitionIndex
    )
    MacrometaOffsetComponents(ledgerId, entryId, partitionIndex).asJson.noSpaces
  }
}

object MacrometaOffset {
  def fromJson(json: String): MacrometaOffset = {
    parser.decode[MacrometaOffsetComponents](json) match {
      case Right(components) =>
        MacrometaOffset(new MessageIdImpl(components.ledgerId, components.entryId, components.partitionIndex))
      case Left(error) =>
        throw new IllegalArgumentException(s"Failed to deserialize MacrometaOffset from JSON: $error")
    }
  }
}

case class MacrometaOffsetComponents(ledgerId: Long, entryId: Long, partitionIndex: Int)
