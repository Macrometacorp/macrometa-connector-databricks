/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.write

import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.connector.write.{PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.mapAsScalaMapConverter

class MacrometaStreamingWrite(options: CaseInsensitiveStringMap, schema: StructType) extends StreamingWrite {
  private val LOGGER = LoggerFactory.getLogger(classOf[MacrometaStreamingWrite])

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    val serializableOptions: Map[String, String] = options.asCaseSensitiveMap.asScala.toMap

    new MacrometaStreamingDataWriterFactory(serializableOptions, schema)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    LOGGER.debug(s"Epoch $epochId completed successfully.")

  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = null
}
