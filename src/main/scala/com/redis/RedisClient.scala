package com.redis

import java.net.InetSocketAddress
import scala.collection.immutable.Queue
import akka.actor._
import akka.event.Logging
import akka.io.{BackpressureBuffer, IO, Tcp, TcpPipelineHandler}
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
      log.debug("sending command: {}", command)

      val req = RedisRequest(sender, command)
      sentRequests :+= req
      pipe ! init.Command(req)
    }

    case init.Event(RedisReplyEvent(replies)) =>
      val len = replies.length
      val reqs = sentRequests.take(len)
      sentRequests = sentRequests.drop(len)
      reqs zip replies map { case (req, reply) =>
        log.info("received reply: {}", (reply.toString.take(30) + (if (reply.toString.length > 30) "..." else "")))
        reply match {
          case err: ErrorReply =>
            log.warning("received error reply: {}", err)
            req.sender ! Status.Failure(err.value)
          case _ =>
            req.sender ! req.command.ret(reply)
        }
      }

    case c: CloseCommand => {
      log.info("Got to close ..")
      flushRequestQueue(pipe)
//      pipe ! init.Command(c)
      context.become(closing(pipe))
    }

    case Terminated(`pipe`) => {
      log.info("connection pipeline closed: {}", pipe)
      context stop self
    }
  }

  def closing(pipe: ActorRef): Receive = {
    case init.Event(evt) => {
      log.info("stopping by {}", evt)
      //      log.info("error cause = " + evt.getErrorCause + " isAborted = " + evt.isAborted + " isConfirmed = " + evt.isConfirmed + " isErrorClosed = " + evt.isErrorClosed + " isPeerClosed = " + evt.isPeerClosed)
      context stop self
    }
  }

  protected def flushRequestQueue(pipe: ActorRef): Unit = {
    pendingRequests foreach (pipe.!)
    pendingRequests = Queue.empty[RedisRequest]
  }

  val init = TcpPipelineHandler.withLogger(log,
    new ResponseParsing() >>
    new RequestRendering() >>
    new BackpressureBuffer(
      lowBytes = 100,
      highBytes = 1000,
      maxBytes = 10000
    )
  )

}
