package com.macrometa.spark.stream.pulsar.macrometa_utils

import org.apache.spark.sql.util.CaseInsensitiveStringMap

class MacrometaUtils {
  def createTopic(options: CaseInsensitiveStringMap): String = {
    val topic = s"persistent://${options.get("tenant")}/c8${options.get("replication")}._system/c8${options.get("replication")}s.${options.get("stream")}"
    topic
  }

  def createTopic(options: Map[String, String]): String = {
    val topic = s"persistent://${options("tenant")}/c8${options("replication")}._system/c8${options("replication")}s.${options("stream")}"
    topic
  }

}
