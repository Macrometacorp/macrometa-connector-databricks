/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.write

import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaWriteBuilder(options: CaseInsensitiveStringMap, schema: StructType) extends WriteBuilder{
  override def buildForStreaming(): StreamingWrite = new MacrometaStreamingWrite(options, schema)
}
