package com.macrometa.spark.stream.write

import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.apache.pulsar.client.api.{Producer, PulsarClient, Schema => PulsarSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String


class MacrometaStreamingDataWriter(options: Map[String, String], schema: StructType) extends DataWriter[InternalRow]{
  val client: PulsarClient = MacrometaPulsarClientInstance.getInstance(federation =
    options.getOrElse("federation", ""),port = options.getOrElse("port",6651.toString), jwtToken = options.getOrElse("jwtToken", "")).getClient

  val topic: String = new MacrometaUtils().createTopic(options)
  private val producer: Producer[Array[Byte]] = client.newProducer(PulsarSchema.BYTES).topic(topic).create()

  override def write(record: InternalRow): Unit = {
    // Convert the InternalRow to a regular Row with the schema
    val row = new GenericRowWithSchema(record.toSeq(schema).toArray, schema)
    val json = rowToJson(row)
    producer.send(json.getBytes)
  }

  override def commit(): WriterCommitMessage = MacrometaWriterCommitMessage

  override def abort(): Unit = producer.close()

  override def close(): Unit = {
    producer.close()
  }

  private def rowToJson(row: Row): String = {
    implicit val anyEncoder: Encoder[Any] = {
      case s: String => Json.fromString(s)
      case utf8: UTF8String => Json.fromString(utf8.toString)
      case i: Int => Json.fromInt(i)
      case l: Long => Json.fromLong(l)
      case d: Double => Json.fromDoubleOrNull(d)
      case f: Float => Json.fromFloatOrNull(f)
      case b: Boolean => Json.fromBoolean(b)
      case _ => Json.Null
    }
    val rowMap = row.getValuesMap[Any](row.schema.fieldNames)
    rowMap.asJson.noSpaces
  }
}
case object MacrometaWriterCommitMessage extends WriterCommitMessage
