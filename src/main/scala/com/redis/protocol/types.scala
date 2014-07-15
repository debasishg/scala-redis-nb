package com.redis
package protocol

import akka.actor.ActorRef
import scala.language.existentials


case class RedisRequest(sender: ActorRef, command: RedisCommand[_])

case class RedisError(message: String) extends Throwable(message)
object Queued extends RedisError("Queued")
object Disconnected extends RedisError("Disconnected")

case object Discarded
