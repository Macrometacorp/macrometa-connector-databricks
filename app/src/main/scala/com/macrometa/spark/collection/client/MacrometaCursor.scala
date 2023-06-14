/*
 * Copyright (c) 2023 Macrometa Corp All rights reserved.
 */

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

class MacrometaCursor(federation: String, apikey: String, fabric: String) {
  implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "SingleRequest")
  implicit val executionContext: ExecutionContextExecutor =
    system.executionContext

  private def request(
      batchSize: Int,
      collection: String,
      endpoint: String,
      method: akka.http.scaladsl.model.HttpMethod,
      query: String
  ): HttpRequest = {
    var customQuery: String = query
    if (query.isEmpty) {
      customQuery = s"FOR doc IN $collection RETURN doc"
    }

    val optionsStream =
      parser.parse("""{"stream": true}""").getOrElse(Json.Null)

    val body: CursorRequestDTO = CursorRequestDTO.apply(
      batchSize = batchSize,
      count = false,
      query = customQuery,
      ttl = 60,
      optionsStream
    )
    val bodyRequest = body.asJson
    val header = headers.RawHeader("Authorization", apikey)
    HttpRequest(
      method = method,
      uri = s"https://api-$federation/_fabric/$fabric/$endpoint",
      entity =
        HttpEntity(ContentTypes.`application/json`, bodyRequest.toString())
    ).withHeaders(header)
  }

  def inferSchema(collection: String, query: String): StructType = {
    val jsonResponse: Json =
      getSampleDocuments(collection = collection, query = query)
    val spark = SparkSession.getActiveSession.get

    val results =
      jsonResponse.hcursor.downField("result").as[Json].getOrElse(Json.arr())

    val schemas: Seq[StructType] = results.asArray.get.map { resultEntity =>
      val jsonAsDataFrame = spark.read.json(
        spark.sparkContext.parallelize(Seq(resultEntity.toString))
      )
      println(jsonAsDataFrame.schema.printTreeString())
      jsonAsDataFrame.schema

    }
    println(s"-----------$schemas")
    findMostCommonSchema(schemas)
  }

  private def getSampleDocuments(collection: String, query: String): Json = {
    val response: Future[HttpResponse] = Http().singleRequest(
      request(
        50,
        collection,
        endpoint = "_api/cursor",
        method = HttpMethods.POST,
        query
      )
    )

    val timeout = 10.seconds
    val res = Await.ready(
      response
        .flatMap(res => res.entity.toStrict(timeout))
        .map(entity => entity.data.utf8String),
      timeout
    )

    val obj = parser.parse(res.value.get.get)
    val json: Json = obj.getOrElse(Json.Null)

    response.onComplete { case Success(value) =>
      Http().shutdownAllConnectionPools()
      system.terminate()
    }

    json
  }

  private def findMostCommonSchema(schemas: Seq[StructType]): StructType = {
    println(schemas)
    if (schemas.isEmpty) {
      new StructType()
    } else {
      val grouped = schemas.groupBy(identity).mapValues(_.size)
      val mostCommon = grouped.maxBy(_._2)._1
      mostCommon
    }
  }

  def executeQuery(
      batchSize: Int,
      collection: String,
      query: String
  ): Iterator[Json] = new Iterator[Json] {

    private var hasMore = true
    private var id: Option[String] = None
    private var documentsIterator: Iterator[Json] = Iterator.empty

    override def hasNext: Boolean =
      documentsIterator.hasNext || fetchNextBatch()

    override def next(): Json = documentsIterator.next()

    private def fetchNextBatch(): Boolean = {
      if (!hasMore) {
        return false
      }

      val endpoint = id match {
        case Some(cursorId) => s"_api/cursor/$cursorId"
        case None           => "_api/cursor"
      }
      val method = id match {
        case Some(_) => HttpMethods.PUT
        case None    => HttpMethods.POST
      }

      val responseCursor: Future[HttpResponse] = Http().singleRequest(
        request(
          batchSize,
          collection,
          endpoint = endpoint,
          method = method,
          query
        )
      )

      val timeout = 15.seconds
      val resCursor = Await.result(
        responseCursor
          .flatMap(res => res.entity.toStrict(timeout))
          .map(entity => entity.data.utf8String),
        timeout
      )

      val jsonCursor: Json = parser.parse(resCursor).getOrElse(null)

      id = jsonCursor.hcursor.downField("id").focus.flatMap(_.asString)
      hasMore = jsonCursor.hcursor
        .downField("hasMore")
        .focus
        .flatMap(_.asBoolean)
        .getOrElse(false)
      documentsIterator = jsonCursor.hcursor
        .downField("result")
        .focus
        .flatMap(_.asArray)
        .getOrElse(Vector.empty)
        .iterator

      documentsIterator.hasNext
    }
  }

}
