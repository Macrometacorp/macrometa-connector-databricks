package com.macrometa.spark.stream.read

import com.macrometa.spark.stream.read.microbatch.MacrometaOffset
import org.apache.spark.sql.connector.read.InputPartition

case class MacrometaInputPartition(start: MacrometaOffset, end: MacrometaOffset) extends InputPartition
