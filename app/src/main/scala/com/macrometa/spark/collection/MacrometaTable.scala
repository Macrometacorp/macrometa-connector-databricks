/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection

import com.macrometa.spark.collection.client.MacrometaValidations
import com.macrometa.spark.collection.reader.MacrometaScanBuilder
import com.macrometa.spark.collection.writer.MacrometaWriteBuilder
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  SupportsWrite,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter

class MacrometaTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String],
    macrometaValidations: MacrometaValidations
) extends Table
    with SupportsRead
    with SupportsWrite {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA
  ).asJava

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    macrometaValidations.validateAPiKeyPermissions(
      collection = options.get("collection"),
      accessLevels = Array("rw", "ro")
    )
    new MacrometaScanBuilder(options, schema)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    macrometaValidations.validateAPiKeyPermissions(
      collection = info.options().get("collection"),
      accessLevels = Array("rw")
    )
    new MacrometaWriteBuilder(info.options(), info.schema())
  }
}
