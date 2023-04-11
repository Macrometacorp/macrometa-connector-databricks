package com.macrometa.spark.collection.writer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.{BatchWrite, SupportsTruncate, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaWriteBuilder(options: CaseInsensitiveStringMap, schema: StructType)
  extends WriteBuilder with SupportsTruncate with Logging{

  override def buildForBatch(): BatchWrite = {
    new MacrometaBatchWriter(options, schema)
  }

  override def truncate(): WriteBuilder = ???
}
