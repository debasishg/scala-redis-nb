package com.redis

import java.net.InetSocketAddress
import scala.collection.immutable.Queue
import akka.actor._
import akka.event.Logging
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
import scala.language.existentials

import com.redis.command.RedisCommand
import scala.annotation.tailrec


object RedisClient {
  import api.RedisOps

  def apply(remote: InetSocketAddress, name: String = "redis-client")(implicit refFactory: ActorRefFactory) =
    new RedisOps(refFactory.actorOf(Props(new RedisClient(remote)), name = name))
}

private class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  private[this] var pendingRequests = Queue.empty[RedisRequest]
  private[this] var sentRequests = Queue.empty[RedisRequest]

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case cmd: RedisCommand =>
      log.info("attempting to send command before connected: {}", cmd)
      pendingRequests :+= RedisRequest(sender, cmd)

    case c @ Connected(remote, _) =>
      log.info(c.toString)
      val connection = sender
      val pipe = context.actorOf(TcpPipelineHandler.props(init, connection, self))
      connection ! Register(pipe)
      context.become(running(pipe))

      log.debug("flushing {} pending requests...", pendingRequests.length)
      flushRequestQueue(pipe)
      log.debug("done.")

    case CommandFailed(c: Connect) =>
      log.error("Connect failed for {} with {}. Stopping... ", c.remoteAddress, c.failureMessage)
      context stop self
  }

  def running(pipe: ActorRef): Receive = {
    case command: RedisCommand => {
      if (pendingRequests.nonEmpty) {
        log.debug("flushing {} requests...", pendingRequests.length)
        flushRequestQueue(pipe)
        log.debug("done.")
      }
      sendRequest(pipe, RedisRequest(sender, command))
    }

    case init.Event(RedisReplyEvent(replies)) =>
      response(replies)

    case c: CloseCommand => {
      log.info("Got to close ..")
      flushRequestQueue(pipe)
      context.become(closing(pipe))
    }

    case Terminated(`pipe`) => {
      log.info("connection pipeline closed: {}", pipe)
      context stop self
    }
  }

  def closing(pipe: ActorRef): Receive = {
    case init.Event(RedisReplyEvent(replies)) =>
      response(replies)
      if (sentRequests.isEmpty)
        pipe ! ConfirmedClose

    case init.Event(evt) => {
      log.info("stopping by {}", evt)
      context stop self
    }

    case Terminated(`pipe`) => {
      log.info("connection pipeline closed: {}", pipe)
      context stop self
    }
  }

  def sendRequest(pipe: ActorRef, req: RedisRequest): Unit = {
    log.debug("sending command: {}", req.command)
    sentRequests :+= req
    pipe ! init.Command(req)
  }

  @tailrec
  final def flushRequestQueue(pipe: ActorRef): Unit =
    if (pendingRequests.nonEmpty) {
      sendRequest(pipe, pendingRequests.head)
      pendingRequests = pendingRequests.tail
      flushRequestQueue(pipe)
    }

  def response(replies: List[RedisReply[_]]): Unit = {
    val len = replies.length
    val reqs = sentRequests.take(len)
    sentRequests = sentRequests.drop(len)
    reqs zip replies map { case (req, reply) =>
      req.sender ! (reply match {
        case err: ErrorReply =>
          Status.Failure(err.value)
        case _ =>
          try req.command.ret(reply)
          catch {
            case e: Throwable =>
              log.error("Error on marshalling result with command: {}, reply: {}", req.command, reply, e)
              Status.Failure(e)
          }
      })
    }
  }


  val init = TcpPipelineHandler.withLogger(log,
    new ResponseParsing() >>
    new RequestRendering() >>
    new BackpressureBuffer(
      lowBytes = 100,
      highBytes = 1000,
      maxBytes = 5000
    )
  )

}
