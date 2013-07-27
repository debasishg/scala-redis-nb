package com.redis

import akka.io.Tcp
import akka.actor.ActorRef
import akka.util.ByteString


package object protocol {

  type Command = Tcp.Command

  type CloseCommand = Tcp.CloseCommand
  val Close = Tcp.Close
  val ConfirmedClose = Tcp.ConfirmedClose
  val Abort = Tcp.Abort

  case class RedisRequest(sender: ActorRef, command: RedisCommand) extends Command


  type Event = Tcp.Event
  type ConnectionClosed = Tcp.ConnectionClosed
  val Closed = Tcp.Closed
  val ConfirmedClosed = Tcp.ConfirmedClosed
  val Aborted = Tcp.Aborted

  case class RedisReplyEvent(replies: List[RedisReply[_]]) extends Event


  case class RedisError(message: String) extends Throwable(message)


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

}
