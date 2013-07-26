package com.redis

import java.net.InetSocketAddress
import scala.collection.immutable.Queue
import akka.actor._
import akka.actor.Status.Failure
import akka.event.Logging
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
import scala.language.existentials
import scala.annotation.tailrec
import com.redis.pipeline.{Serializing, Deserializing}
import protocol._


object RedisClient {
  import api.RedisOps

  def apply(remote: InetSocketAddress, name: String = "redis-client")(implicit refFactory: ActorRefFactory) =
    new RedisOps(refFactory.actorOf(Props(new RedisClient(remote)), name = name))
}

private class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

  private val log = Logging(context.system, this)
  private[this] var pendingRequests = Queue.empty[RedisRequest]
  private[this] var sentRequests = Queue.empty[RedisRequest]

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {
    case cmd: RedisCommand =>
      log.info("Attempting to send command before connected: {}", cmd)
      addPendingRequest(cmd)

    case c @ Connected(remote, _) =>
      log.info(c.toString)
      val connection = sender
      val pipe = context.actorOf(TcpPipelineHandler.props(init, connection, self))
      connection ! Register(pipe)
      context become (running(pipe))
      flushRequestQueue(pipe)

    case CommandFailed(c: Connect) =>
      log.error("Connect failed for {} with {}. Stopping... ", c.remoteAddress, c.failureMessage)
      context stop self

    case x =>
      log.error("Unknown command: {}", x)
      context stop self
  }

  def running(pipe: ActorRef): Receive = {
    case command: RedisCommand =>
      sendRequest(pipe, RedisRequest(sender, command))

    case init.Event(RedisReplyEvent(replies)) =>
      response(replies)

    case init.Event(BackpressureBuffer.HighWatermarkReached) =>
      log.info("Backpressure is too high. Start buffering...")
      context become (buffering(pipe))

    case c: CloseCommand =>
      log.info("Got to close ..")
      flushRequestQueue(pipe)
      context become (closing(pipe))

    case Terminated(`pipe`) => {
      log.info("Connection pipeline closed: {}", pipe)
      context stop self
    }

    case x =>
      log.error("Unknown command: {}", x)
      context stop self
  }

  def buffering(pipe: ActorRef): Receive = {
    case cmd: RedisCommand =>
      addPendingRequest(cmd)

    case init.Event(BackpressureBuffer.LowWatermarkReached) =>
      log.info("Client backpressure became lower, resuming...")
      context become running(pipe)
      flushRequestQueue(pipe)

    case x =>
      log.error("Unknown command: {}", x)
      context stop self
  }

  def closing(pipe: ActorRef): Receive = {
    case init.Event(RedisReplyEvent(replies)) =>
      response(replies)
      if (sentRequests.isEmpty)
        pipe ! ConfirmedClose

    case init.Event(evt) => {
      log.warning("Stopping by {}", evt)
      context stop self
    }

    case Terminated(`pipe`) => {
      log.warning("Connection pipeline closed: {}", pipe)
      context stop self
    }

    case x =>
      log.error("Unknown command: {}", x)
      context stop self
  }

  def addPendingRequest(cmd: RedisCommand): Unit = {
    pendingRequests :+= RedisRequest(sender, cmd)
  }

  def sendRequest(pipe: ActorRef, req: RedisRequest): Unit = {
    log.debug("Sending command: {}", req.command)
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

  @tailrec
  final def response(replies: List[RedisReply[_]]): Unit =
    if (replies.nonEmpty) {
      val req = sentRequests.head
      val reply = replies.head

      log.debug("Received response: {}", reply)

      req.sender ! (reply match {
        case err: ErrorReply =>
          Failure(err.value)

        case _ =>
          try req.command.ret(reply)
          catch {
            case e: Throwable =>
              log.error("Error on marshalling {} requested by {}\n{}:\n{}", reply, req.command, e, e.getStackTraceString)
              Failure(e)
          }
      })

      sentRequests = sentRequests.tail
      response(replies.tail)
    }

  val init = TcpPipelineHandler.withLogger(log,
    new Deserializing() >>
    new Serializing() >>
    new BackpressureBuffer(
      lowBytes = 100,
      highBytes = 4000,
      maxBytes = 5000
    )
  )

}
