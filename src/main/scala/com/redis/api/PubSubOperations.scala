package com.redis.api

import akka.actor.ActorRef
import akka.util.Timeout
import com.redis.serialization.Stringified

trait PubSubOperations { this: RedisOps =>
  import com.redis.protocol.PubSubCommands._
  import akka.pattern.ask

  /**
   * SUBSCRIBE
   * Subscribes the client to the specified channels.
   * Once the client enters the subscribed state it is not supposed to issue any other commands,
   * except for additional SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE commands.
   *
   * Any command, except of QUIT, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE will fail
   * with [[com.redis.RedisConnection.CommandRejected]] cause.
   *
   * The actor passed in the listener parameter will receive messages from all channels
   * the underlying client is subscribed to. It means, every listeners registered with the
   * client will receive messages from all channels, no matter which combinations of
   * listeners and channels were passed to the call(s).
   *
   * The result of the command is not available directly. The listener will receive one or
   * more [[Subscribed]] events in the case of success.
   */
  def subscribe( listener: ActorRef, channels: Seq[String] )(implicit timeout: Timeout) = {
    clientRef.tell( Subscribe(channels), listener )
  }

  def subscribe( listener: ActorRef, channel: String, channels: String* )(implicit timeout: Timeout) = {
    clientRef.tell( Subscribe(channel, channels: _*), listener )
  }

  /**
   * UNSUBSCRIBE
   * Unsubscribes the client from the given channels, or from all of them if none is given.
   * When no channels are specified, the client is unsubscribed from all the previously subscribed channels.
   * In this case, a message for every unsubscribed channel will be sent to the client.
   *
   * The operation affects all listeners registered with the client. A result of the operation will be
   * reported to listeners via [[Unsubscribed]] messages.
   */
  def unsubscribe(channels: String* )(implicit timeout: Timeout) : Unit = {
    clientRef ! Unsubscribe(channels)
  }

  /**
   * PUBLISH
   * Posts a message to the given channel.
   */
  def publish(channel: String, message: Stringified)(implicit timeout: Timeout) = {
    clientRef.ask( Publish(channel, message) ).mapTo[Publish#Ret]
  }

  /**
   * PSUBSCRIBE
   * Subscribes the client to the given patterns.
   *
   * See [[subscribe()]] for more details.
   */
  def psubscribe(listener: ActorRef, patterns: Seq[String])(implicit timeout: Timeout) = {
    clientRef.tell( PSubscribe(patterns), listener )
  }

  def psubscribe( listener: ActorRef, pattern: String, patterns: String*)(implicit timeout: Timeout) = {
    clientRef.tell( PSubscribe(pattern, patterns:_*), listener )
  }

  /**
   * PUNSUBSCRIBE
   * Unsubscribes the client from the given patterns, or from all of them if none is given.
   */
  def punsubscribe( patterns: String*)(implicit timeout: Timeout) = {
    clientRef ! PUnsubscribe(patterns)
  }

}
