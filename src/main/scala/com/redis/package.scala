package com

import akka.actor.ActorRef
import akka.io.Tcp
import com.redis.command.RedisCommand


package object redis {

  type Command = Tcp.Command

  type CloseCommand = Tcp.CloseCommand
  val Close = Tcp.Close
  val ConfirmedClose = Tcp.ConfirmedClose
  val Abort = Tcp.Abort

  case class RedisRequest(sender: ActorRef, command: RedisCommand) extends Command


  type Event = Tcp.Event

  case class RedisReplyEvent(replies: List[RedisReply]) extends Event
}
