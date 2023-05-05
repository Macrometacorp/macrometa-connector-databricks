/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.utils

import io.circe.Json
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructType}

class InternalRowJsonConverter(schema: StructType) {
  private val jsonFields: Array[(String, Json)] = schema.fields.zipWithIndex.map { case (field, index) =>
    (field.name, Json.Null)
  }

  def internalRowToJson(row: InternalRow): Json = {
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      val fieldValue = field.dataType match {
        case StringType => Json.fromString(row.getString(index))
        case IntegerType => Json.fromInt(row.getInt(index))
        case LongType => Json.fromLong(row.getLong(index))
        case DoubleType => Json.fromDoubleOrNull(row.getDouble(index))
        case BooleanType => Json.fromBoolean(row.getBoolean(index))
        case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${field.dataType}")
      }
      jsonFields(index) = jsonFields(index).copy(_2 = fieldValue)
    }
    Json.fromFields(jsonFields)
  }
}

