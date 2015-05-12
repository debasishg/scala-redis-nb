package com.redis.protocol

import akka.util.ByteString

object PubSubCommands {
  import com.redis.serialization._
  import DefaultFormats._

  sealed trait PushedMessage {
    def destination: String
  }
  case class Subscribed(destination: String, isPattern: Boolean, numOfSubscriptions: Int) extends PushedMessage
  case class Unsubscribed(destination: String, isPattern: Boolean, numOfSubscriptions: Int) extends PushedMessage
  case class Message(destination: String, payload: ByteString) extends PushedMessage
  case class PMessage(pattern: String, destination: String, payload: ByteString) extends PushedMessage

  abstract class PubSubCommand(_cmd: String) extends RedisCommand[Unit](_cmd)(PartialDeserializer.UnitDeserializer)
  abstract class SubscribeCommand(_cmd: String) extends PubSubCommand(_cmd)

  case class Subscribe(channels: Seq[String]) extends SubscribeCommand("SUBSCRIBE") {
    require(channels.nonEmpty, "Channels should not be empty")
    def params = channels.toArgs
  }

  object Subscribe {
    def apply(channel: String, channels: String*) = new Subscribe( channel +: channels )
  }

  case class PSubscribe( patterns: Seq[String] ) extends SubscribeCommand("PSUBSCRIBE") {
    require(patterns.nonEmpty, "Patterns should not be empty")
    def params = patterns.toArgs
  }

  object PSubscribe {
    def apply(pattern: String, patterns: String*) : PSubscribe = PSubscribe( pattern +: patterns )
  }

  case class Unsubscribe(channels: Seq[String]) extends PubSubCommand("UNSUBSCRIBE") {
    override def params = channels.toArgs
  }

  case class PUnsubscribe(patterns: Seq[String]) extends PubSubCommand("PUNSUBSCRIBE") {
    override def params = patterns.toArgs
  }

  case class Publish(channel: String, value: Stringified) extends RedisCommand[Int]("PUBLISH") {
    override def params: Args = channel +: value +: ANil
  }

}
