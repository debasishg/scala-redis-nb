package com.redis

import java.net.{InetAddress, InetSocketAddress}
import scala.collection.immutable.Queue
import akka.actor._
import akka.actor.Status.Failure
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
import scala.language.existentials
import scala.annotation.tailrec
import pipeline._
import protocol._


object RedisClient {
  import api.RedisOps

  def apply(remote: InetSocketAddress, name: String = defaultName)(implicit refFactory: ActorRefFactory): RedisOps =
    new RedisOps(refFactory.actorOf(Props(new RedisClient(remote)), name = name))

  private def defaultName = "redis-client-" + nameSeq.next
  private val nameSeq = Iterator from 0
}

private class RedisClient(remote: InetSocketAddress) extends Actor with ActorLogging {
  import Tcp._
  import context.system

  private[this] var pendingRequests = Queue.empty[RedisRequest]
  private[this] var sentRequests = Queue.empty[RedisRequest]

  IO(Tcp) ! Connect(remote)

  def receive = unconnected

  def unconnected: Receive = {
    case cmd: RedisCommand =>
      log.info("Attempting to send command before connected: {}", cmd)
      addPendingRequest(cmd)

    case c @ Connected(remote, _) =>
      log.info("Connected to redis server {}:{}.", remote.getHostName, remote.getPort)
      val connection = sender
      val pipe = context.actorOf(TcpPipelineHandler.props(init, connection, self), name = "pipeline")
      connection ! Register(pipe)
      context become (running(pipe))
      flushRequestQueue(pipe)

    case CommandFailed(c: Connect) =>
      log.error("Connect failed for {} with {}. Stopping... ", c.remoteAddress, c.failureMessage)
      context stop self
  }

  def running(pipe: ActorRef): Receive = withTerminationManagement {
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
  }

  def buffering(pipe: ActorRef): Receive = withTerminationManagement {
    case cmd: RedisCommand =>
      log.debug("Received a command while buffering: {}", cmd)
      addPendingRequest(cmd)

    case init.Event(BackpressureBuffer.LowWatermarkReached) =>
      log.info("Client backpressure became lower, resuming...")
      context become running(pipe)
      flushRequestQueue(pipe)
  }

  def closing(pipe: ActorRef): Receive = withTerminationManagement {
    case init.Event(RedisReplyEvent(replies)) =>
      response(replies)
      if (sentRequests.isEmpty)
        pipe ! ConfirmedClose

    case init.Event(evt) =>
      log.warning("Stopping by {}", evt)
  }

  def withTerminationManagement(handler: Receive): Receive = handler orElse {
    case Terminated(x) => {
      log.info("Child termination detected: {}", x)
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

        case _: RedisReply[_] =>
          try req.command.ret(reply)
          catch {
            case e: Throwable =>
              log.error("Error on marshalling {} requested by {}\n{}:\n{}",
                reply, req.command, e, e.getStackTraceString)
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
