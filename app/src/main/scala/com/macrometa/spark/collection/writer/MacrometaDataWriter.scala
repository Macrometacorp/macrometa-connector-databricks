/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.writer

import akka.Done
import com.macrometa.spark.collection.client.{ImportDataDTO, MacrometaImport}
import com.macrometa.spark.collection.utils.InternalRowJsonConverter
import io.circe.Json
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class MacrometaDataWriter(options: Map[String, String], schema: StructType) extends DataWriter[InternalRow] with Logging {

  private val converter = new InternalRowJsonConverter(schema)
  private val buffer = new ListBuffer[Json]()
  private var insertManyFuture: Option[Future[Done]] = None

  override def write(record: InternalRow): Unit = {
    val jsonDocument = converter.internalRowToJson(record)
    buffer += jsonDocument
  }

  override def commit(): WriterCommitMessage = {

    val data : ImportDataDTO = ImportDataDTO.apply(data = buffer, primaryKey = options.getOrElse("primaryKey",""))
    val client = new MacrometaImport(federation = options("regionUrl"),
      apikey = options("apiKey"), fabric = options("fabric"))
    insertManyFuture = Some(client.insertMany(collection = options("collection"), body = data))
    buffer.clear()
    null
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    insertManyFuture.foreach { future =>
      val timeout = 5.minutes // Adjust the timeout as needed
      Await.result(future, timeout)
    }
  }
}


case class MacrometaWriterCommitMessage(status: String) extends WriterCommitMessage
