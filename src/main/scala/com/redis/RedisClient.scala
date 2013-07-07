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

class RedisClient(remote: InetSocketAddress) extends Actor {
  import Tcp._
  import context.system

  val log = Logging(context.system, this)
  var buffer = Vector.empty[RedisCommand]
  val promiseQueue = new scala.collection.mutable.Queue[RedisCommand]
  IO(Tcp) ! Connect(remote)

  def receive = baseHandler

  def baseHandler: Receive = {
    case CommandFailed(c: Connect) =>
      log.error("Connect failed for " + c.remoteAddress + " with " + c.failureMessage + " stopping ..")
      context stop self

    case c@Connected(remote, local) =>
      log.info(c.toString)
      val connection = sender
      connection ! Register(self)

      context become {
        case command: RedisCommand => { 
          log.info("sending command to Redis: " + command)

          // flush if anything there in buffer before processing next command
          if (!buffer.isEmpty) buffer.foreach(c => sendRedisCommand(connection, c))

          // now process the current command
          sendRedisCommand(connection, command)
        }

        case Received(data) => 
          log.info("got response from Redis: " + data.decodeString("UTF-8"))
          val responseArray = data.toArray[Byte]
          val replies = splitReplies(responseArray)
          replies.map {r =>
            promiseQueue.dequeue execute r
          }

        case CommandFailed(w: Write) => {
          log.error("Write failed for " + w.data.decodeString("UTF-8"))

          // write failed : switch to buffering mode: NACK with suspension
          connection ! ResumeWriting
          context become buffering(connection)
        }
        case "close" => {
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

  def buffering(conn: ActorRef): Receive = {
    var peerClosed = false
    var pingAttempts = 0

    {
      case command: RedisCommand => {
        buffer :+= command 
        pingAttempts += 1
        if (pingAttempts == 10) {
          log.info("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        }
      }
      case PeerClosed => peerClosed = true
      case WritingResumed => {
        if (peerClosed) {
          log.info("Can't recover .. closing and discarding buffered commands")
          conn ! Close
          context stop self
        } else {
          context become baseHandler
        }
      }
    }
  }

  def sendRedisCommand(conn: ActorRef, command: RedisCommand)(implicit ec: ExecutionContext) = {
    promiseQueue += command
    if (!buffer.isEmpty) buffer = buffer drop 1
    conn ! Write(ByteString(command.line))
    val f = command.promise.future
    pipe(f) to sender
  }
}

