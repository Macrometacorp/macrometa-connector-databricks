/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.write

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.types.StructType

class MacrometaStreamingDataWriterFactory(
    options: Map[String, String],
    schema: StructType
) extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long
  ): DataWriter[InternalRow] = new MacrometaStreamingDataWriter(options, schema)
}
