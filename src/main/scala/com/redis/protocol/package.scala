package com.redis

import akka.io.Tcp
import akka.actor.ActorRef
import akka.util.ByteString
import scala.language.existentials
import com.redis.serialization.{Writer, Stringified}


package object protocol {

  type Command = Tcp.Command

  type CloseCommand = Tcp.CloseCommand
  val Close = Tcp.Close
  val ConfirmedClose = Tcp.ConfirmedClose
  val Abort = Tcp.Abort

  case class RedisRequest(sender: ActorRef, command: RedisCommand[_]) extends Command


  type Event = Tcp.Event
  type ConnectionClosed = Tcp.ConnectionClosed
  val Closed = Tcp.Closed
  val ConfirmedClosed = Tcp.ConfirmedClosed
  val Aborted = Tcp.Aborted

  case object RequestQueueEmpty extends Event

  case class RedisError(message: String) extends Throwable(message)
  object Queued extends RedisError("Queued") {
    def unapply(q: Queued.type) = Some("Queued")
  }

  case object Discarded


  type Args = RedisCommand.Args

  val ANil = RedisCommand.Args.empty

  implicit class StringifiedArgsOps(values: Seq[Stringified]) {
    def toArgs = new Args(values)
  }

  implicit class ArgsOps[A: Writer](values: Seq[A]) {
    def toArgs = new Args(values)
  }

  /**
   * Response codes from the Redis server
   */
  val Cr      = '\r'.toByte
  val Lf      = '\n'.toByte
  val Status  = '+'.toByte
  val Integer = ':'.toByte
  val Bulk    = '$'.toByte
  val Multi   = '*'.toByte
  val Err     = '-'.toByte

  val Newline = ByteString("\r\n")

  val NullBulkReplyCount = -1
  val NullMultiBulkReplyCount = -1

}
