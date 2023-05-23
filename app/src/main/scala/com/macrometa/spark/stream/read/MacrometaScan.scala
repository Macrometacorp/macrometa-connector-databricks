/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.read

import com.macrometa.spark.stream.read.microbatch.MacrometaMicroBatchStream
import org.apache.spark.sql.connector.read.streaming.{
  ContinuousStream,
  MicroBatchStream
}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaScan(options: CaseInsensitiveStringMap, schema: StructType)
    extends Scan {
  override def readSchema(): StructType = ???

  override def toBatch: Batch = ???

  override def toMicroBatchStream(
      checkpointLocation: String
  ): MicroBatchStream = new MacrometaMicroBatchStream(options, schema)

  override def toContinuousStream(
      checkpointLocation: String
  ): ContinuousStream = super.toContinuousStream(checkpointLocation)

}
