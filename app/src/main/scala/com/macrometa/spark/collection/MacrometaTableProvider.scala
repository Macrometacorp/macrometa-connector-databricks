package com.macrometa.spark.collection

import com.macrometa.spark.collection.client.MacrometaCursor
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class MacrometaTableProvider extends TableProvider with DataSourceRegister{

  private def macrometaClient(option: CaseInsensitiveStringMap): MacrometaCursor = {
    new MacrometaCursor(federation = option.get("regionUrl"),
      apikey = option.get("apikey"), fabric = option.get("fabric"))
  }

  override def shortName(): String = "macrometa"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    macrometaClient(options).inferSchema(collection = options.get("collection"), query = options.getOrDefault("query",""))

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table =
    new MacrometaTable(schema = schema, partitioning = partitioning, properties = properties)
}
