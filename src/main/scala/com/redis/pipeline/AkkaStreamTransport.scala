package com.redis.pipeline

import java.net.InetSocketAddress

import akka.actor.{Status, ActorRef, Actor}
import akka.stream.actor.ActorProducer
import akka.stream.io.StreamTcp
import akka.stream.io.StreamTcp.Connect
import akka.stream.scaladsl.Flow
import akka.stream.{FlowMaterializer, MaterializerSettings}
import akka.util.ByteString
import com.redis.protocol.RedisRequest
import org.reactivestreams.api.{Consumer, Producer}

import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success, Try}


trait AkkaStreamTransport { me: Actor =>

  private val materializerSettings = MaterializerSettings()
  private val flowMaterializer = FlowMaterializer(materializerSettings)

  private def requestResponseHandler = RequestResponseHandler(context.system)
  private def createPipe = context watch context.actorOf(TcpPipeProducer.props())

  private def returnToSender(commanderAndResult: (ActorRef, Try[Any])): Unit = {
    commanderAndResult match {
      case (commander, Success(res)) => commander ! res
      case (commander, Failure(e)) => commander ! Status.Failure(e)
    }
  }

  private def consumeResponses(responseChannel: Producer[ByteString], handler: RequestResponseHandler): Flow[Unit] =
    Flow(responseChannel)
      .mapConcat(handler.deserialize)
      .foreach(returnToSender)

  private def commandProducer(pipe: ActorRef): Producer[RedisRequest] =
    ActorProducer[RedisRequest](pipe)

  private def requestProducer(pipe: ActorRef, handler: RequestResponseHandler): Producer[ByteString] =
    Flow(commandProducer(pipe))
      .map(handler.serialize)
      .toProducer(flowMaterializer)

  def connectionMessage(server: InetSocketAddress): Connect =
    StreamTcp.Connect(materializerSettings, server)

  def initiateConnection(upstream: Consumer[ByteString], downstream: Producer[ByteString]): (ActorRef, Future[Option[Throwable]]) = {
    val pipe = createPipe
    val promise = Promise[Option[Throwable]]()
    val handler = requestResponseHandler

    requestProducer(pipe, handler).produceTo(upstream)
    consumeResponses(downstream, handler).onComplete(flowMaterializer) { tu =>
      handler.disconnect()
      promise.success(tu.failed.toOption)
    }
    (pipe, promise.future)
  }
}
