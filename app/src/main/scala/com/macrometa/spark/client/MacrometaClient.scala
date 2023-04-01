package com.macrometa.spark.client

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http

import scala.util.{Failure, Success}
import akka.http.scaladsl.model.{HttpMethods, _}
import io.circe.generic.auto._
import io.circe.{Json, parser}
import io.circe.syntax._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, DurationInt, SECONDS}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class MacrometaClient(federation: String, apikey: String, fabric: String){
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "SingleRequest")
  implicit val executionContext: ExecutionContextExecutor = system.executionContext

  private def request(batchSize: Int, collection: String, endpoint: String, method: akka.http.scaladsl.model.HttpMethod, query: String ): HttpRequest = {
    var customQuery : String = query
    if (query.isEmpty){
      customQuery = s"FOR doc IN $collection RETURN doc"
    }

    val optionsStream = parser.parse("""{"stream": true}""").getOrElse(Json.Null)

    val body: CursorRequestDTO = CursorRequestDTO.apply(batchSize = batchSize, count = false, query = customQuery, ttl = 600, optionsStream)
    val bodyRequest = body.asJson
    val header = headers.RawHeader("Authorization", apikey)
    HttpRequest(method = method, uri = s"https://api-$federation/_fabric/$fabric/$endpoint", entity = HttpEntity(ContentTypes.`application/json`,
      bodyRequest.toString())).withHeaders(header)
  }

  def infer_schema(collection: String): StructType = {
    val jsonResponse: Json = start_request(batchSize = 1, collection = collection, query = "")
    val resultEntity : Json = jsonResponse.asObject.get("result").get
    val spark = SparkSession.getActiveSession.get
    val jsonAsDataFrame = spark.read.json(spark.sparkContext.parallelize(Seq(resultEntity.toString)))
    jsonAsDataFrame.schema
  }

  def start_request(batchSize: Int, collection: String, query: String): Json = {
    val response: Future[HttpResponse] = Http().singleRequest(request(batchSize, collection, endpoint = "_api/cursor", method = HttpMethods.POST, query))
    val timeout = 10.seconds
    val res = Await.ready(response.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)
    val obj = parser.parse(res.value.get.get)
    val json: Json = obj.getOrElse(null)

    response.onComplete {
      case Success(value) =>
        println("Request executed successfully " + value.status.value)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }
    json
  }

  def cursor_start_request(batchSize: Int, collection: String, query: String): Json = {

    val response: Future[HttpResponse] = Http().singleRequest(request(batchSize, collection, endpoint = "_api/cursor", method = HttpMethods.POST, query))
    val timeout = 10.seconds
    val res = Await.ready(response.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)
    val obj = parser.parse(res.value.get.get)
    val json: Json = obj.getOrElse(null)  //complete cursor

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
    println(listOfDocuments.asArray.size)

    response.onComplete {
      case Success(value) =>
        println("Request executed successfully " + value.status.value)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }
    listOfDocuments
  }



  def insertMany(collection:String, body: ImportDataDTO): Unit ={
    val header = headers.RawHeader("Authorization", apikey)
    val request = HttpRequest(method = HttpMethods.POST, uri = s"https://api-$federation/_fabric/$fabric/_api/import/$collection", entity = HttpEntity(ContentTypes.`application/json`,
      body.asJson.toString())).withHeaders(header)

    val response: Future[HttpResponse] = Http().singleRequest(request)

    val timeout = 10.seconds
    Await.ready(response.flatMap(res => res.entity.toStrict(timeout)).map(entity => entity.data.utf8String), timeout)

    response.onComplete {
      case Success(value) =>
        println("Request executed successfully " + value.status.value)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }
  }

  def insertManyV2(collection: String, body: ImportDataDTO): Unit = {
    val batchSize = 100

    def createRequest(jsonBatch: Json): HttpRequest = {
      HttpRequest(
        method = HttpMethods.POST,
        uri = s"https://api-$federation/_fabric/$fabric/_api/import/$collection",
        entity = HttpEntity(ContentTypes.`application/json`, jsonBatch.toString())
      ).withHeaders(headers.RawHeader("Authorization", apikey))
    }

    def executeRequest(request: HttpRequest): Future[HttpResponse] = {
      Http().singleRequest(request)
    }

    val jsonBatches = body.data.toList.grouped(batchSize)

    val responseFutures: List[Future[HttpResponse]] = jsonBatches.map { batch =>
      val jsonBatch = Json.obj(
        "data" -> batch.asJson,
        "details" -> Json.fromBoolean(body.details),
        "primaryKey" -> Json.fromString(body.primaryKey),
        "replace" -> Json.fromBoolean(body.replace)
      )
      val request = createRequest(jsonBatch)
      executeRequest(request)
    }.toList

    val aggregatedResponsesFuture: Future[List[HttpResponse]] = Future.sequence(responseFutures)
    val timeout = Duration(10 * responseFutures.size, SECONDS)
    val aggregatedResponses: List[HttpResponse] = Await.result(aggregatedResponsesFuture, timeout)

    aggregatedResponses.foreach { response =>
      response.status match {
        case status if status.isSuccess() =>
          println("Request executed successfully " + status.value)
        case status =>
          println(s"Request failed with status ${status.value}")
      }
    }

    Http().shutdownAllConnectionPools()
    system.terminate()
  }

}