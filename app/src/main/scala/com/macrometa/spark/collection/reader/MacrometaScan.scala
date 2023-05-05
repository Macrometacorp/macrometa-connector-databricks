/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.reader

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

class MacrometaScan(options: CaseInsensitiveStringMap, schema: StructType)  extends Scan with Batch{

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new MacrometaCollectionPartition(options))
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    val serializableOptions = options.asCaseSensitiveMap().asScala.toMap
    new MacrometaPartitionReaderFactory(serializableOptions, schema =schema)}

  override def readSchema(): StructType = ???

  override def toBatch: Batch = this
}
