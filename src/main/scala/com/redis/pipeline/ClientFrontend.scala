package com.redis.pipeline

import akka.io._
import com.redis.protocol._
import scala.collection.immutable.Queue
import scala.annotation.tailrec
import akka.actor.Status.Failure
import akka.io.TcpPipelineHandler.WithinActorContext
import akka.actor.ActorRef


class ClientFrontend extends PipelineStage[WithinActorContext, Command, Command, Event, Event] {

  def apply(ctx: WithinActorContext) = new PipePair[Command, Command, Event, Event] {
    import ctx.{getLogger => log}

    private val redisClientRef: ActorRef = ctx.getContext.self

    private[this] var sentRequests = Queue.empty[RedisRequest]

    @tailrec
    private def response(replies: List[RedisReply[_]]): Unit =
      if (replies.nonEmpty) {
        val RedisRequest(commander, cmd) = sentRequests.head
        val reply = replies.head

        log.info("Received response: {}", reply)

        val result = reply match {
          case err: ErrorReply =>
            Failure(err.value)

          case r: RedisReply[_] =>
            try cmd.ret(r)
            catch {
              case e: Throwable =>
                log.error(e, "Error on marshalling {} requested by {}", r, cmd)
                Failure(e)
            }
        }

        commander.tell(result, redisClientRef)

        sentRequests = sentRequests.tail
        response(replies.tail)
      }

    val commandPipeline = (cmd: Command) => cmd match {
      case req: RedisRequest =>
        log.debug("Sending {}, previous head: {}", req.command, sentRequests.headOption.map(_.command))
        sentRequests :+= req
        ctx singleCommand req

      case _ => ctx singleCommand cmd
    }

    val eventPipeline = (evt: Event) => evt match {
      case RedisReplyEvent(replies: List[RedisReply[_]]) =>
        log.info("Received {} replies, current head: {}", replies.length, sentRequests.headOption.map(_.command))
        response(replies)
        ctx.nothing

      case _ => ctx singleEvent evt
    }
  }
}
