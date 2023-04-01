package com.macrometa.spark.client

import io.circe.Json

import scala.collection.mutable.ListBuffer

case class ImportDataDTO(data: ListBuffer[Json], details: Boolean = false, primaryKey: String = "", replace: Boolean = false)
