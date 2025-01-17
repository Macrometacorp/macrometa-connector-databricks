/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.client

import io.circe.Json

import scala.collection.mutable.ListBuffer

case class ImportDataDTO(
    data: ListBuffer[Json],
    details: Boolean = false,
    primaryKey: String = "",
    replace: Boolean = false
)
