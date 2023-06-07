/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.utils

import io.circe.Json
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DataType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.unsafe.types.UTF8String

class InternalRowJsonConverter(schema: StructType) {
  def internalRowToJson(row: InternalRow): Json = {

    def valueToJson(value: Any, dataType: DataType): Json = {
      if (value == null) {
        Json.Null
      } else {
        dataType match {
          case StringType =>
            Json.fromString(value.asInstanceOf[UTF8String].toString)
          case IntegerType => Json.fromInt(value.asInstanceOf[Int])
          case LongType    => Json.fromLong(value.asInstanceOf[Long])
          case DoubleType  => Json.fromDoubleOrNull(value.asInstanceOf[Double])
          case FloatType   => Json.fromFloatOrNull(value.asInstanceOf[Float])
          case BooleanType => Json.fromBoolean(value.asInstanceOf[Boolean])
          case structType: StructType =>
            internalRowToJsonObject(value.asInstanceOf[InternalRow], structType)
          case ArrayType(elementType, _) =>
            val arrayData = value.asInstanceOf[ArrayData]
            val jsonArray = arrayData
              .toArray(elementType)
              .map(valueToJson(_, elementType))
              .toSeq
            Json.fromValues(jsonArray)
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported data type: $dataType"
            )
        }
      }
    }

    def internalRowToJsonObject(row: InternalRow, schema: StructType): Json = {
      val fields = schema.fields.zipWithIndex.map { case (field, index) =>
        val fieldValue =
          valueToJson(row.get(index, field.dataType), field.dataType)
        (field.name, fieldValue)
      }
      Json.fromFields(fields)
    }

    internalRowToJsonObject(row, schema)
  }

}
