package com.macrometa.spark.stream.read.microbatch

import com.macrometa.spark.stream.read.MacrometaInputPartition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class MacrometaMicroBatchPartitionReaderFactory(options: Map[String, String], schema: StructType) extends PartitionReaderFactory{

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val macrometaPartition = partition.asInstanceOf[MacrometaInputPartition]
    new MacrometaMicroBatchPartitionReader(options, schema, macrometaPartition)
  }
}
