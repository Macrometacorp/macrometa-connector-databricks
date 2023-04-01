package com.macrometa.spark.reader

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaScanBuilder(options: CaseInsensitiveStringMap,schema: StructType) extends ScanBuilder{
  override def build(): Scan = new MacrometaScan(options, schema)
}
