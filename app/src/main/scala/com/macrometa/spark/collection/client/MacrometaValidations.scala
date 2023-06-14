package com.macrometa.spark.collection.client
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, _}
import io.circe.syntax.EncoderOps
import io.circe.{Json, parser}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

class MacrometaValidations(federation: String, apikey: String, fabric: String) {
  implicit val system: ActorSystem = ActorSystem("my-system")

  import system.dispatcher
  val parallelism = 32

  private def request(
      httpMethod: HttpMethod,
      endpoint: String,
      fabric: String = "",
      entity: Option[String] = None
  ): Future[String] = {
    val header = headers.RawHeader("Authorization", apikey)
    var url = s"https://api-$federation/_fabric/$fabric/_api/$endpoint"
    if (fabric.equalsIgnoreCase("")) {
      url = s"https://api-$federation/_api/$endpoint"
    }
    // Create the HTTP request
    val httpReq = HttpRequest(
      method = httpMethod,
      uri = url,
      headers = List(header)
    )

    // If there's an entity, add it to the request
    val httpReqWithEntity = entity match {
      case Some(e) =>
        httpReq.withEntity(HttpEntity(ContentTypes.`application/json`, e))
      case None => httpReq
    }

    val response: Future[HttpResponse] = Http().singleRequest(httpReqWithEntity)

    val timeout = 10.seconds

    val res = Await.ready(
      response
        .flatMap(res => res.entity.toStrict(timeout))
        .map(entity => entity.data.utf8String),
      timeout
    )

    res
  }

  def validateFabric(): Unit = {
    val response = request(HttpMethods.GET, "database/metadata", fabric)
    val obj = parser.parse(response.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)
    if (json.asObject.get("error").get.asBoolean.get) {
      val err = json.asObject.get("errorMessage").get
      val code = json.asObject.get("code").get
      throw new IllegalArgumentException(
        s"Invalid fabric ${fabric}, error with message ${err}, Status code: ${code}"
      )

    }
  }

  def validateCollection(collection: String): Unit = {
    val response = request(HttpMethods.GET, "collection", fabric)
    val obj = parser.parse(response.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)
    if (json.asObject.get("error").get.asBoolean.get) {
      val err = json.asObject.get("errorMessage").get
      val code = json.asObject.get("code").get
      throw new IllegalArgumentException(
        s"Error with message ${err}, Status code: ${code}"
      )

    }
    val resultArray = json.asObject.get("result").flatMap(_.asArray)

    resultArray match {
      case Some(array) =>
        val collectionExists = array.exists(jsonObj =>
          jsonObj.asObject.get("name").get.asString.get == collection
        )
        if (!collectionExists) {
          throw new IllegalArgumentException(
            s"Error with collection ${collection}, does not exists."
          )
        }
      case None =>
        println("result is not an array or does not exist")
    }
  }
  def validateStream(stream: String, replicationType: String): Unit = {
    var rep: Boolean = false
    if (replicationType.equalsIgnoreCase("global")) {
      rep = true
    }
    val response = request(HttpMethods.GET, s"streams?global=${rep}", fabric)
    val obj = parser.parse(response.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)
    if (json.asObject.get("error").get.asBoolean.get) {
      val err = json.asObject.get("errorMessage").get
      val code = json.asObject.get("code").get
      throw new IllegalArgumentException(
        s"Error with message ${err}, Status code: ${code}"
      )
    }

    val resultArray = json.asObject.get("result").flatMap(_.asArray)

    resultArray match {
      case Some(array) =>
        val streamExists = array.exists(jsonObj =>
          jsonObj.asObject
            .get("topic")
            .get
            .asString
            .get == s"c8${replicationType}s.${stream}"
        )
        if (!streamExists) {
          throw new IllegalArgumentException(
            s"Error with stream ${stream}, does not exists."
          )
        }
      case None =>
        println("result is not an array or does not exist")
    }
  }

  def validateQuery(query: String): Unit = {
    val queryJson: Json = Json.obj(
      "query" -> query.asJson
    )
    val response =
      request(HttpMethods.POST, "query", fabric, Some(queryJson.noSpaces))
    val obj = parser.parse(response.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)
    if (json.asObject.get("error").get.asBoolean.get) {
      val err = json.asObject.get("errorMessage").get
      val code = json.asObject.get("code").get
      throw new IllegalArgumentException(
        s"Error with message for wrong query:  ${err}, Status code: ${code}"
      )
    }
  }

  def validateAPiKeyPermissions(
      collection: String,
      accessLevels: Array[String]
  ): Unit = {
    val words = apikey.split("\\.")
    val apikeyId = words(words.length - 2)

    val response = request(
      HttpMethods.GET,
      s"key/${apikeyId}/database/${fabric}/collection/${collection}"
    )
    val obj = parser.parse(response.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)
    if (json.asObject.get("error").get.asBoolean.get) {
      val err = json.asObject.get("errorMessage").get
      val code = json.asObject.get("code").get
      throw new IllegalArgumentException(
        s"Error with message for wrong apikey:  ${err}, Status code: ${code}"
      )
    }
    json.asObject.flatMap(_("result")).flatMap(_.asString) match {
      case Some(permission)
          if accessLevels
            .map(_.toLowerCase)
            .contains(permission.toLowerCase) => // do nothing
      case _ =>
        throw new IllegalArgumentException(
          s"Apikey with insufficient permissions"
        )
    }
  }

}
