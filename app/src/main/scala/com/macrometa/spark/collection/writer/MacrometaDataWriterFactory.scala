/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

case class MacrometaDataWriterFactory(
    options: Map[String, String],
    schema: StructType
) extends DataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long
  ): DataWriter[InternalRow] = {
    new MacrometaDataWriter(options, schema)
  }
}
