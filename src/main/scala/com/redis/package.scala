package com

import akka.actor.ActorRef
import akka.io.Tcp
import com.redis.command.RedisCommand
import scala.language.existentials


package object redis {

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
}
