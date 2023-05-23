/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  SupportsReportPartitioning
}
import org.apache.spark.sql.types.StructType

class MacrometaPartitionReaderFactory(
    options: Map[String, String],
    schema: StructType
) extends PartitionReaderFactory
    with SupportsReportPartitioning {
  override def outputPartitioning(): Partitioning = ???

  override def readSchema(): StructType = schema

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val customPartition = partition.asInstanceOf[MacrometaCollectionPartition]
    new MacrometaCollectionPartitionReader(customPartition, options, schema)
  }
}
