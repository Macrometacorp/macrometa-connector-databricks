package com.macrometa.spark.writer

import com.macrometa.spark.client.{ImportDataDTO, MacrometaClient}
import io.circe.Json
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class MacrometaDataWriter(options: Map[String, String], schema: StructType) extends DataWriter[InternalRow] with Logging {

  private val buffer = new ListBuffer[Json]()

  val client = new MacrometaClient(federation = options("federation"),
    apikey = options("apiKey"), fabric = options("fabric"))

  override def write(record: InternalRow): Unit = {
    val jsonDocument = internalRowToJson(record, schema)
    buffer += jsonDocument
  }

  override def commit(): WriterCommitMessage = {
    val data : ImportDataDTO = ImportDataDTO.apply(data = buffer, primaryKey = options.getOrElse("primaryKey",""))
    client.insertManyV2(collection = options("collection"), body = data)
    buffer.clear()
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}

  def internalRowToJson(row: InternalRow, schema: StructType): Json = {
    val jsonFields = schema.fields.zipWithIndex.map { case (field, index) =>

      val fieldName = field.name
      val fieldValue = field.dataType match {
        case StringType => Json.fromString(row.getString(index))
        case IntegerType => Json.fromInt(row.getInt(index))
        case LongType => Json.fromLong(row.getLong(index))
        case DoubleType => Json.fromDoubleOrNull(row.getDouble(index))
        case BooleanType => Json.fromBoolean(row.getBoolean(index))
        case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${field.dataType}")
      }
      (fieldName, fieldValue)
    }.toMap
    Json.fromFields(jsonFields)
  }
}


case class MacrometaWriterCommitMessage(status: String) extends WriterCommitMessage