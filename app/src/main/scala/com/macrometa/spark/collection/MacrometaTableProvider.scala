/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.collection

import com.macrometa.spark.collection.client.{
  MacrometaCursor,
  MacrometaValidations
}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class MacrometaTableProvider extends TableProvider with DataSourceRegister {

  private def macrometaClient(
      option: CaseInsensitiveStringMap
  ): MacrometaCursor = {
    new MacrometaCursor(
      federation = option.get("regionUrl"),
      apikey = option.get("apikey"),
      fabric = option.get("fabric")
    )
  }

  private def macrometaValidations(
      option: CaseInsensitiveStringMap
  ): MacrometaValidations = {
    new MacrometaValidations(
      federation = option.get("regionUrl"),
      apikey = option.get("apikey"),
      fabric = option.get("fabric")
    )
  }

  override def shortName(): String = "macrometa"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val requiredProperties = Seq("regionUrl", "apiKey", "fabric", "collection")
    val collection = options.get("collection")
    val defaultQuery = s"FOR doc IN $collection RETURN doc"
    requiredProperties.foreach { propName =>
      val propValue = options.get(propName)
      if (propValue == null || propValue.isEmpty) {
        throw new IllegalArgumentException(s"Option '$propName' is required")
      }
    }
    macrometaValidations(options).validateFabric()
    macrometaValidations(options).validateCollection(options.get("collection"))
    macrometaValidations(options).validateQuery(
      options.getOrDefault("query", defaultQuery)
    )

    macrometaClient(options).inferSchema(
      collection = collection,
      query = options.getOrDefault("query", defaultQuery)
    )
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    val caseInsensitiveProperties = new CaseInsensitiveStringMap(properties)

    new MacrometaTable(
      schema = schema,
      partitioning = partitioning,
      properties = properties,
      macrometaValidations = macrometaValidations(caseInsensitiveProperties)
    )
  }
}
