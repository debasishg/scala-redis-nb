package com.redis.protocol

import com.redis.serialization._


object TransactionCommands {
  import DefaultWriters._

  case object Multi extends RedisCommand[Boolean]("MULTI") {
    def params = ANil
  }

  // the type of RedisCommand is not used. It can be anything for which Format exists
  case object Exec extends RedisCommand[List[Array[Byte]]]("EXEC") {
    def params = ANil
  }

  case object Discard extends RedisCommand[Boolean]("DISCARD") {
    def params = ANil
  }

  case class Watch(keys: Seq[String]) extends RedisCommand[Boolean]("WATCH") {
    require(keys.nonEmpty, "Keys should not be empty")
    def params = keys.toArgs
  }

  case object Unwatch extends RedisCommand[Boolean]("UNWATCH") {
    def params = ANil
  }

  object Watch {
    def apply(key: String, keys: String*): Watch = Watch(key +: keys)
  }
}
