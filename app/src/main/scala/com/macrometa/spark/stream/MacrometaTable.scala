/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream

import com.macrometa.spark.stream.read.MacrometaScanBuilder
import com.macrometa.spark.stream.write.MacrometaWriteBuilder
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
import java.util.Arrays.asList

class MacrometaTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
) extends Table
    with SupportsWrite
    with SupportsRead {
  override def name(): String = "macrometa"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    new util.HashSet[TableCapability](
      asList(
        TableCapability.BATCH_WRITE,
        TableCapability.TRUNCATE,
        TableCapability.STREAMING_WRITE,
        TableCapability.ACCEPT_ANY_SCHEMA,
        TableCapability.BATCH_READ,
        TableCapability.MICRO_BATCH_READ,
        TableCapability.CONTINUOUS_READ
      )
    );

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new MacrometaWriteBuilder(info.options(), info.schema())

  override def newScanBuilder(
      options: CaseInsensitiveStringMap
  ): ScanBuilder = {
    new MacrometaScanBuilder(options, schema)
  }
}
