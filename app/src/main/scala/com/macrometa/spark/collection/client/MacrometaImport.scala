package com.macrometa.spark.collection.client

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import io.circe.Json
import io.circe.syntax._

import scala.concurrent.Future

class MacrometaImport(federation: String, apikey: String, fabric: String){
  def insertMany(collection: String, body: ImportDataDTO, batchSize : Int = 100): Future[Done] = {
    implicit val system: ActorSystem = ActorSystem("my-system")
    import system.dispatcher
    val parallelism = 32

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
    val source: Source[Json, NotUsed] = Source.fromIterator(() => jsonBatches.map { batch =>
      Json.obj(
        "data" -> batch.asJson,
        "details" -> Json.fromBoolean(body.details),
        "primaryKey" -> Json.fromString(body.primaryKey),
        "replace" -> Json.fromBoolean(body.replace)
      )
    })


    val flow: Flow[Json, HttpResponse, NotUsed] = Flow[Json]
      .map(createRequest)
      .mapAsync(parallelism)(executeRequest)

    val sink: Sink[HttpResponse, Future[Done]] = Sink.foreach { response =>
      response.status match {
        case status if status.isSuccess() =>
        case status =>
      }
      response.discardEntityBytes()
    }

    val runnableGraph: RunnableGraph[Future[Done]] = source.via(flow).toMat(sink)(Keep.right)

    val done: Future[Done] = runnableGraph.run()

    done.onComplete { _ =>
      Http().shutdownAllConnectionPools()
      system.terminate()
    }
    done
  }
}
