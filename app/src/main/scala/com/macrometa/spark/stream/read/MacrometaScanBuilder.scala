/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.read

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaScanBuilder(options: CaseInsensitiveStringMap, schema: StructType) extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {
  override def build(): Scan = new MacrometaScan(options, schema)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???

  override def pushedFilters(): Array[Filter] = ???

  override def pruneColumns(requiredSchema: StructType): Unit = ???
}
