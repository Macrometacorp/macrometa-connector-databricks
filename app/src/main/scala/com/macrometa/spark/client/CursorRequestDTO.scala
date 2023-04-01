package com.macrometa.spark.client

import io.circe.Json

case class CursorRequestDTO(val batchSize: Int, val count: Boolean, val query: String, val ttl: Int, options: Json)
