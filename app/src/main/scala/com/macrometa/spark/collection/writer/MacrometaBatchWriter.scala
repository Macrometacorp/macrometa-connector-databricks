/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters.mapAsScalaMapConverter

class MacrometaBatchWriter(options: CaseInsensitiveStringMap, schema: StructType) extends BatchWrite with
  SupportsTruncate with Logging{
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    MacrometaDataWriterFactory(options.asCaseSensitiveMap().asScala.toMap, schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  override def truncate(): WriteBuilder = ???
}
