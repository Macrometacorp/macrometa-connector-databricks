package com.macrometa.spark.collection.reader

import com.macrometa.spark.collection.client.MacrometaCursor
import io.circe.{Json, parser}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class MacrometaCollectionPartitionReader(inputPartition: MacrometaCollectionPartition, options: Map[String, String], schema: StructType)   extends PartitionReader[InternalRow]{

  val client = new MacrometaCursor(federation = options("federation"),
    apikey = options("apiKey"), fabric = options("fabric"))

  val response: Json = client.executeQuery(batchSize =
    options.getOrElse("batchSize",100.toString).toInt,
    collection = options("collection"), options.getOrElse("query", ""))

  private val dataIterator : Iterator[InternalRow] = jsonToInternalRowIterator(response.toString(), schema)

  override def next(): Boolean = dataIterator.hasNext

  override def get(): InternalRow = {
    dataIterator.next()
  }

  override def close(): Unit = {

  }

  def jsonToInternalRowIterator(jsonString: String, schema: StructType): Iterator[InternalRow] = {
    parser.parse(jsonString) match {
      case Left(parseFailure) =>
        throw new RuntimeException(s"Failed to parse JSON: ${parseFailure.getMessage}")
      case Right(json) =>
        json.asArray match {
          case None =>
            throw new RuntimeException("Expected a JSON array")
          case Some(jsonArray) =>
            jsonArray.iterator.map { jsonObject =>
              val values = schema.fields.map { field =>
                val fieldName = field.name
                val fieldType = field.dataType
                val fieldValue: Json = jsonObject.hcursor.downField(fieldName).focus.getOrElse(Json.Null)

                fieldType match {
                  case StringType => UTF8String.fromString(fieldValue.asString.getOrElse(null))
                  case LongType => fieldValue.asNumber.flatMap(_.toLong).getOrElse(null)
                }
              }
              InternalRow.fromSeq(values)
            }
        }
    }
  }

}
