package com.macrometa.spark.stream.pulsar

import org.apache.pulsar.client.api.{AuthenticationFactory, PulsarClient}

import java.util.concurrent.TimeUnit

class MacrometaPulsarClientInstance private(pulsarUrl: String, jwtToken: String) {
  private lazy val client: PulsarClient = {
      PulsarClient.builder()
      .serviceUrl(pulsarUrl)
      .authentication(AuthenticationFactory.token(jwtToken))
      .connectionTimeout(10000, TimeUnit.MILLISECONDS).build()
  }
  def getClient: PulsarClient = client
}

object MacrometaPulsarClientInstance{
  @volatile private var instance: Option[MacrometaPulsarClientInstance] = None

  def getInstance(pulsarUrl: String, jwtToken: String): MacrometaPulsarClientInstance = {
    instance match {
      case Some(client) => client
      case None =>
        synchronized{
          instance match {
            case Some(client) => client
            case None =>
              val newInstance = new MacrometaPulsarClientInstance(pulsarUrl, jwtToken)
              instance = Some(newInstance)
              newInstance
          }
        }
    }
  }
}
