package com.macrometa.spark.stream

import com.macrometa.spark.stream.pulsar.MacrometaPulsarClientInstance
import com.macrometa.spark.stream.pulsar.macrometa_utils.MacrometaUtils
import org.apache.pulsar.client.api.PulsarClient
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util


class MacrometaTableProvider extends TableProvider with DataSourceRegister{

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new MacrometaTable(schema, partitioning, properties)

  override def shortName(): String = "macrometa"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val client: PulsarClient = MacrometaPulsarClientInstance.getInstance(federation = options.get("federation"),
      port = options.getOrDefault("port",6651.toString), jwtToken = options.get("jwtToken")).getClient
    new MacrometaUtils().inferSchema(client = client, options = options)
  }
}
