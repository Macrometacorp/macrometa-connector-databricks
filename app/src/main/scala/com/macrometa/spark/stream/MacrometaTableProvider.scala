/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream

import com.macrometa.spark.collection.client.MacrometaValidations
import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import org.apache.pulsar.client.api.PulsarClient
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class MacrometaTableProvider extends TableProvider with DataSourceRegister {

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = new MacrometaTable(schema, partitioning, properties)

  override def shortName(): String = "macrometa"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val requiredProperties = Seq(
      "regionUrl",
      "token",
      "fabric",
      "tenant",
      "stream",
      "replication",
      "subscriptionName"
    )

    requiredProperties.foreach { propName =>
      val propValue = options.get(propName)
      if (propValue == null || propValue.isEmpty) {
        throw new IllegalArgumentException(s"Option '$propName' is required")
      }
    }

    val replication = options.getOrDefault("replication", "global")
    if (
      !(replication
        .equalsIgnoreCase("global") || replication.equalsIgnoreCase("local"))
    ) {
      throw new IllegalArgumentException(
        "Replication type should be either global or local"
      )
    }

    val macrometaClient = new MacrometaValidations(
      federation = options.get("regionUrl"),
      apikey = s"bearer ${options.get("token")}",
      fabric = options.get("fabric")
    )
    macrometaClient.validateFabric()
    macrometaClient.validateStream(
      options.get("stream"),
      options.get("replication")
    )

    val client: PulsarClient = MacrometaPulsarClientInstance
      .getInstance(
        federation = options.get("regionUrl"),
        port = options.getOrDefault("port", 6651.toString),
        jwtToken = options.get("token")
      )
      .getClient
    new MacrometaUtils().inferSchema(client = client, options = options)
  }
}
