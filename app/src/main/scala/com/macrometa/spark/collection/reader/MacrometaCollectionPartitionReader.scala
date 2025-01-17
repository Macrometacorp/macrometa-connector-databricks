/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.reader

import com.macrometa.spark.collection.client.MacrometaCursor
import io.circe.{Json, parser}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  DoubleType,
  FloatType,
  LongType,
  NullType,
  StringType,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

class MacrometaCollectionPartitionReader(
    inputPartition: MacrometaCollectionPartition,
    options: Map[String, String],
    schema: StructType
) extends PartitionReader[InternalRow] {

  val client = new MacrometaCursor(
    federation = options("regionUrl"),
    apikey = options("apiKey"),
    fabric = options("fabric")
  )

  private val documentsIterator: Iterator[Json] = client.executeQuery(
    batchSize = options("batchSize").toInt,
    collection = options("collection"),
    query = options("query")
  )

  private val dataIterator: Iterator[InternalRow] =
    new Iterator[InternalRow] {
      private var internalRowIterator: Iterator[InternalRow] = Iterator.empty

      override def hasNext: Boolean =
        internalRowIterator.hasNext || fetchNextBatch()

      override def next(): InternalRow = internalRowIterator.next()

      private def fetchNextBatch(): Boolean = {
        if (!documentsIterator.hasNext) {
          false
        } else {
          val jsonBatch = documentsIterator.next()
          val internalRowIteratorOpt = jsonToInternalRowIterator(jsonBatch.toString(), schema)
          if (internalRowIteratorOpt.isEmpty) {
            // Skip this batch and fetch the next one
            fetchNextBatch()
          }
          else {
            internalRowIterator = internalRowIteratorOpt
            true
          }
        }
      }
    }

  def next(): Boolean = dataIterator.hasNext

  def get(): InternalRow = dataIterator.next()

  override def close(): Unit = {}

  def jsonToInternalRowIterator(
      jsonString: String,
      schema: StructType
  ): Iterator[InternalRow] = {
    parser.parse(jsonString) match {
      case Left(parseFailure) =>
        throw new RuntimeException(
          s"Failed to parse JSON: ${parseFailure.getMessage}"
        )
      case Right(json) =>
        val jsonArray = json.asArray.getOrElse(Vector(json))
        jsonArray.iterator.map { jsonObject =>
          val valuesOpt = schema.fields.map { field =>
            val fieldName = field.name
            val fieldType = field.dataType
            val fieldValueOpt: Option[Json] = jsonObject.hcursor
              .downField(fieldName)
              .focus

            fieldValueOpt
              .flatMap { fieldValue =>
                jsonToDataType(fieldValue, fieldType)
              }
          }
          if (valuesOpt.contains(None)) {
            None
          } else {
            Some(InternalRow.fromSeq(valuesOpt.map(_.get)))
          }
        }.collect { case Some(internalRow) => internalRow }
    }
  }

  def jsonToDataType(json: Json, dataType: DataType): Option[Any] = {
    dataType match {
      case StringType  => if (json.isNull) Some(null) else json.asString.map(UTF8String.fromString)
      case LongType    => if (json.isNull) Some(null) else json.asNumber.flatMap(_.toLong)
      case DoubleType  => if (json.isNull) Some(null) else json.asNumber.map(_.toDouble)
      case FloatType   => if (json.isNull) Some(null) else json.asNumber.map(_.toFloat)
      case NullType    => if (json.isNull) Some(null) else None
      case BooleanType => if (json.isNull) Some(null) else json.asBoolean
      case ArrayType(elementType, _) =>
        if (json.isNull) Some(null) else json.asArray.map { jsonArray =>
          new GenericArrayData(
            jsonArray.map(jsonElement =>
              jsonToDataType(jsonElement, elementType).orNull
            )
          )
        }
      case structType: StructType =>
        if (json.isNull) Some(null) else json.asObject.map(obj => {
          val rowValues = structType.fields.flatMap(field => {
            jsonToDataType(obj(field.name).getOrElse(Json.Null), field.dataType)
          })
          InternalRow.fromSeq(rowValues)
        })
      case _ => None
    }
  }

}
