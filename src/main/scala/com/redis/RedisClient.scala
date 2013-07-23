package com.redis

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await}
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import akka.pattern.{ask, pipe}
import akka.event.Logging
import akka.io.{Tcp, IO}
import akka.util.{ByteString, Timeout}
import akka.actor._
import ExecutionContext.Implicits.global
import scala.language.existentials

import ProtocolUtils._
import RedisReplies._


object RedisClient {
  import api.RedisOps

  type CloseCommand = Tcp.CloseCommand
  val Close = Tcp.Close
  val ConfirmedClose = Tcp.ConfirmedClose
  val Abort = Tcp.Abort

  def apply(remote: InetSocketAddress, name: String = "redis-client")(implicit refFactory: ActorRefFactory) =
    new RedisOps(refFactory.actorOf(Props(new RedisClient(remote)), name = name))
}

private class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  private[this] var buffer = Vector.empty[RedisCommand]
  private[this] val promiseQueue = new scala.collection.mutable.Queue[RedisCommand]

  IO(Tcp) ! Connect(remote)

  def receive = baseHandler

  protected def baseHandler: Receive = {
    case CommandFailed(c: Connect) =>
      log.error("Connect failed for " + c.remoteAddress + " with " + c.failureMessage + " stopping ..")
      context stop self

    case command: RedisCommand =>
      log.warning("attempting to send command before connected: " + command)
      buffer :+= command

    case c@Connected(remote, local) =>
      log.info(c.toString)
      val connection = sender
      connection ! Register(self)

      context become {
        case command: RedisCommand => { 
          log.debug("sending command to Redis: " + command)

          // flush if anything there in buffer before processing next command
          if (!buffer.isEmpty) buffer.foreach(c => sendRedisCommand(connection, c))

          // now process the current command
          sendRedisCommand(connection, command)
        }

        case Received(data) => 
          log.debug("got response from Redis: " + data.utf8String)
          val replies = splitReplies(data)
          replies.map {r =>
            promiseQueue.dequeue execute r
          }

        case CommandFailed(w: Write) => {
          log.error("Write failed for " + w.data.utf8String)

          // write failed : switch to buffering mode: NACK with suspension
          connection ! ResumeWriting
          context become buffering(connection)
        }
        case Close => {
          log.info("Got to close ..")
          if (!buffer.isEmpty) buffer.foreach(c => sendRedisCommand(connection, c))
          connection ! Close
        }
        case c: ConnectionClosed => {
          log.info("stopping ..")
          log.info("error cause = " + c.getErrorCause + " isAborted = " + c.isAborted + " isConfirmed = " + c.isConfirmed + " isErrorClosed = " + c.isErrorClosed + " isPeerClosed = " + c.isPeerClosed)
          context stop self
        }
      }
  }

  protected def buffering(conn: ActorRef): Receive = {
    var peerClosed = false
    var pingAttempts = 0

    {
      case command: RedisCommand => {
        buffer :+= command 
        pingAttempts += 1
        if (pingAttempts == 10) {
          log.warning("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        }
      }
      case PeerClosed => peerClosed = true
      case WritingResumed => {
        if (peerClosed) {
          log.warning("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        } else {
          context become baseHandler
        }
      }
    }
  }

  protected def sendRedisCommand(conn: ActorRef, command: RedisCommand)(implicit ec: ExecutionContext) = {
    promiseQueue += command
    if (!buffer.isEmpty) buffer = buffer drop 1
    conn ! Write(command.line)
    val f = command.promise.future
    pipe(f) to sender
  }
}
