/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

package com.macrometa.spark.stream.pulsar

import org.apache.pulsar.client.api.{AuthenticationFactory, PulsarClient}

import java.util.concurrent.TimeUnit
import scala.collection.mutable

class MacrometaPulsarClientInstance private (
    pulsarUrl: String,
    apikey: String
) {
  private lazy val client: PulsarClient = {
    PulsarClient
      .builder()
      .serviceUrl(pulsarUrl)
      .authentication(AuthenticationFactory.token(apikey))
      .connectionTimeout(10000, TimeUnit.MILLISECONDS)
      .build()
  }
  def getClient: PulsarClient = client
}

object MacrometaPulsarClientInstance {
  private val instances
      : mutable.Map[(String, String, String), MacrometaPulsarClientInstance] =
    mutable.Map()

  def getInstance(
      federation: String,
      port: String,
      apikey: String
  ): MacrometaPulsarClientInstance = {
    val pulsarUrl = s"pulsar+ssl://api-$federation:$port"
    instances.get((federation, port, apikey)) match {
      case Some(client) => client
      case None =>
        synchronized {
          instances.get((federation, port, apikey)) match {
            case Some(client) => client
            case None =>
              val newInstance =
                new MacrometaPulsarClientInstance(pulsarUrl, apikey)
              instances((federation, port, apikey)) = newInstance
              newInstance
          }
        }
    }
  }
}
