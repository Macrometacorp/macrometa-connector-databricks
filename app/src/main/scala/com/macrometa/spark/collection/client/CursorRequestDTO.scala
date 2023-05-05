/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection.client

import io.circe.Json

case class CursorRequestDTO(val batchSize: Int, val count: Boolean, val query: String, val ttl: Int, options: Json)
