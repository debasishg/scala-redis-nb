package com.redis

import java.net.InetSocketAddress
import scala.collection.immutable.Queue
import akka.actor._
import akka.event.Logging
import akka.io.{IO, Tcp, TcpPipelineHandler}
import scala.language.existentials

import com.redis.command.RedisCommand


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

  def receive = baseHandler

  protected def baseHandler: Receive = {
    case cmd: RedisCommand =>
      log.info("attempting to send command before connected: " + cmd)
      pendingRequests :+= RedisRequest(sender, cmd)

    case c @ Connected(remote, _) =>
      log.info(c.toString)
      val connection = sender
      val pipe = context.actorOf(TcpPipelineHandler.props(init, connection, self))
      connection ! Register(pipe)
      context.become(running(pipe))
      flushRequestQueue(pipe)

    case CommandFailed(c: Connect) =>
      log.error("Connect failed for " + c.remoteAddress + " with " + c.failureMessage + " stopping ..")
      context stop self
  }

  def running(pipe: ActorRef): Receive = {
    case command: RedisCommand => {
      log.debug("sending command to Redis: " + command)

      val req = RedisRequest(sender, command)
      sentRequests :+= req
      pipe ! init.Command(req)
    }

    case init.Event(RedisReplyEvent(replies)) =>
      log.debug("received {} replies: {}", replies.length)

      val len = replies.length
      val reqs = sentRequests.take(len)
      sentRequests = sentRequests.drop(len)
      reqs zip replies map { case (req, rep) =>
        req.sender ! req.command.ret(rep)
      }

    case c: CloseCommand => {
      log.info("Got to close ..")
      flushRequestQueue(pipe)
      pipe ! init.Command(c)
    }

    case evt: ConnectionClosed => {
      log.info("stopping ..")
      log.info("error cause = " + evt.getErrorCause + " isAborted = " + evt.isAborted + " isConfirmed = " + evt.isConfirmed + " isErrorClosed = " + evt.isErrorClosed + " isPeerClosed = " + evt.isPeerClosed)
      context stop self
    }

    case Terminated(`pipe`) => {
      log.info("connection pipeline closed: {}", pipe)
      context stop self
    }

    case x => log.error("Unknown command: {}", x)
  }

  protected def flushRequestQueue(pipe: ActorRef): Unit = {
    pendingRequests foreach (pipe.!)
    pendingRequests = Queue.empty[RedisRequest]
  }

  val init = TcpPipelineHandler.withLogger(log,
    new ResponseParsing() >>
    new RequestRendering()
  )

}
