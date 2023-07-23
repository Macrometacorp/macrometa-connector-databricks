/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.write

import com.macrometa.spark.collection.client.MacrometaValidations
import org.apache.spark.sql.connector.write.streaming.{
  StreamingDataWriterFactory,
  StreamingWrite
}
import org.apache.spark.sql.connector.write.{
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.mapAsScalaMapConverter

class MacrometaStreamingWrite(
    options: CaseInsensitiveStringMap,
    schema: StructType
) extends StreamingWrite {
  private val LOGGER = LoggerFactory.getLogger(classOf[MacrometaStreamingWrite])

  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo
  ): StreamingDataWriterFactory = {
    val serializableOptions: Map[String, String] =
      options.asCaseSensitiveMap.asScala.toMap
    val requiredProperties = Seq(
      "regionUrl",
      "apikey",
      "fabric",
      "tenant",
      "stream",
      "replication"
    )

    requiredProperties.foreach { propName =>
      val propValue = options.get(propName)
      if (propValue == null || propValue.isEmpty) {
        throw new IllegalArgumentException(s"Option '$propName' is required")
      }
    }

    val replication = options.get("replication")
    if (
      !(replication
        .equalsIgnoreCase("global") || replication.equalsIgnoreCase("local"))
    ) {
      throw new IllegalArgumentException(
        "Replication type should be either global or local"
      )
    }

    val macrometaClient = new MacrometaValidations(
      federation = options.get("regionUrl"),
      apikey = s"apikey ${options.get("apikey")}",
      fabric = options.get("fabric")
    )
    val streamName = s"c8${options.get("replication")}s.${options.get("stream")}"
    macrometaClient.validateAPiKeyPermissions(
      stream = streamName,
      accessLevels = Array("rw")
    )
    macrometaClient.validateFabric()
    macrometaClient.validateStream(
      options.get("stream"),
      options.get("replication")
    )
    new MacrometaStreamingDataWriterFactory(serializableOptions, schema)
  }

  override def commit(
      epochId: Long,
      messages: Array[WriterCommitMessage]
  ): Unit = {
    LOGGER.debug(s"Epoch $epochId completed successfully.")

  }

  override def abort(
      epochId: Long,
      messages: Array[WriterCommitMessage]
  ): Unit = null
}
