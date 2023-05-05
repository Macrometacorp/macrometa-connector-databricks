package com.macrometa.spark.collection.client

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http

import scala.util.Success
import akka.http.scaladsl.model.{HttpMethods, _}
import io.circe.generic.auto._
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class MacrometaCursor(federation: String, apikey: String, fabric: String){
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SingleRequest")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  private def request(batchSize: Int, collection: String, endpoint: String, method: akka.http.scaladsl.model.HttpMethod, query: String ): HttpRequest = {
    var customQuery : String = query
    if (query.isEmpty){
      customQuery = s"FOR doc IN $collection RETURN doc"
    }

    val optionsStream = parser.parse("""{"stream": true}""").getOrElse(Json.Null)

    val body: CursorRequestDTO = CursorRequestDTO.apply(batchSize = batchSize, count = false, query = customQuery, ttl = 60, optionsStream)
    val bodyRequest = body.asJson
    val header = headers.RawHeader("Authorization", apikey)
    HttpRequest(method = method, uri = s"https://api-$federation/_fabric/$fabric/$endpoint", entity = HttpEntity(ContentTypes.`application/json`,
      bodyRequest.toString())).withHeaders(header)
  }

  def inferSchema(collection: String, query: String): StructType = {
    val jsonResponse: Json = getSampleDocument(collection = collection, query = query)
    val resultEntity : Json = jsonResponse.asObject.get("result").get
    val spark = SparkSession.getActiveSession.get
    val jsonAsDataFrame = spark.read.json(spark.sparkContext.parallelize(Seq(resultEntity.toString)))
    jsonAsDataFrame.schema
  }

  private def getSampleDocument(collection: String, query: String): Json = {
    val response: Future[HttpResponse] = Http().singleRequest(request(1, collection, endpoint = "_api/cursor", method = HttpMethods.POST, query))
    val timeout = 10.seconds
    val res = Await.ready(response.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)
    val obj = parser.parse(res.value.get.get)
    val json: Json = obj.getOrElse(null)

    response.onComplete {
      case Success(value) =>
        Http().shutdownAllConnectionPools()
        system.terminate()
    }
    json
  }

  def executeQuery(batchSize: Int, collection: String, query: String): Json = {

    val response: Future[HttpResponse] = Http().singleRequest(request(batchSize, collection, endpoint = "_api/cursor", method = HttpMethods.POST, query))
    val timeout = 10.seconds
    val res = Await.ready(response.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)
    val obj = parser.parse(res.value.get.get)
    val json: Json = obj.getOrElse(null)

    var listOfDocuments: Json = json.asObject.get("result").get

    var hasMore: Boolean = json.asObject.get("hasMore").get.asBoolean.get
    var id = ""

    if(json.asObject.get("id").nonEmpty){
      id = json.asObject.get("id").get.asString.get
    }


    if(hasMore){
      do {
        val responseCursor: Future[HttpResponse] = Http().singleRequest(request(batchSize, collection, endpoint = s"_api/cursor/$id", method = HttpMethods.PUT, query))
        val timeout = 10.seconds
        val resCursor = Await.ready(responseCursor.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)
        val objCursor = parser.parse(resCursor.value.get.get)
        val jsonCursor: Json = objCursor.getOrElse(null)
        hasMore = jsonCursor.asObject.get("hasMore").get.asBoolean.get
        val merged = listOfDocuments.asArray.getOrElse(Vector.empty) ++ jsonCursor.asObject.get("result").get.asArray.getOrElse(Vector.empty)
        listOfDocuments = Json.fromValues(merged)
      } while (hasMore)
    }

    response.onComplete {
      case Success(value) =>
        Http().shutdownAllConnectionPools()
        system.terminate()
    }
    listOfDocuments
  }


}
