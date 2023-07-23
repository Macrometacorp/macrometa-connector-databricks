/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{
  BatchWrite,
  SupportsTruncate,
  WriteBuilder
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaWriteBuilder(
    options: CaseInsensitiveStringMap,
    schema: StructType
) extends WriteBuilder
    with SupportsTruncate
    with Logging {

  override def buildForBatch(): BatchWrite = {
    val batchSize = options.getOrDefault("batchSize", "100").toInt
    if (batchSize < 1 || batchSize > 10000) {
      throw new IllegalArgumentException(
        "Batch size should be greater than 0 and less or equal to 10000"
      )
    }
    new MacrometaBatchWriter(options, schema)
  }

  override def truncate(): WriteBuilder = ???
}
