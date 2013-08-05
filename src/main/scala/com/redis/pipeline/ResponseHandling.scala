package com.redis.pipeline

import akka.io._
import akka.io.TcpPipelineHandler.WithinActorContext
import akka.actor.ActorRef
import akka.actor.Status.Failure
import akka.util.CompactByteString
import com.redis.protocol._
import com.redis.serialization.{PartialDeserializer, Deserializer}
import com.redis.serialization.Deserializer.Result
import scala.collection.immutable.Queue
import scala.annotation.tailrec
import scala.language.existentials


class ResponseHandling extends PipelineStage[WithinActorContext, Command, Command, Event, Event] {

  def apply(ctx: WithinActorContext) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    private[this] val parser = new Deserializer()
    private[this] val redisClientRef: ActorRef = ctx.getContext.self
    private[this] var sentRequests = Queue.empty[RedisRequest]

    private def handleResponse(data: CompactByteString): Iterable[Result] = {
      @tailrec
      def parseAndDispatch(data: CompactByteString): Iterable[Result] =
        if (sentRequests.isEmpty) ctx.singleEvent(RequestQueueEmpty)
        else {
          val RedisRequest(commander, cmd) = sentRequests.head
          parser.parse(data, cmd.des) match {
            case Result.Ok(reply, remaining) =>
              val result = reply match {
                case err: RedisError => Failure(err)

                case _ => reply
              }
              log.debug("RESULT: {}", result)
              commander.tell(result, redisClientRef)
              sentRequests = sentRequests.tail
              parseAndDispatch(remaining)

            case Result.NeedMoreData => ctx.singleEvent(RequestQueueEmpty)

            case Result.Failed(err, data) =>
              log.error(err, "Failed to parse response: {}", data.utf8String.replace("\r\n", "\\r\\n"))
              ctx.singleCommand(Close)
          }
        }

      log.debug("Received data from server: {}", data.utf8String.replace("\r\n", "\\r\\n"))
      parseAndDispatch(data)
    }


    val commandPipeline = (cmd: Command) => cmd match {
      case req: RedisRequest =>
        log.debug("Sending {}, previous head: {}", req.command, sentRequests.headOption.map(_.command))
        sentRequests :+= req
        ctx singleCommand req

      case _ => ctx singleCommand cmd
    }

    val eventPipeline = (evt: Event) => evt match {
      case Tcp.Received(data: CompactByteString) => handleResponse(data)

      case _ => ctx singleEvent evt
    }
  }
}
