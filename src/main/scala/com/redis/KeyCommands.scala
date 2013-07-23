package com.redis

import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.util.ByteString


object KeyCommands {
  case class Keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]) extends KeyCommand {
    type Ret = List[A]
    val line = multiBulk("KEYS" +: Seq(format.apply(pattern)))
    val ret  = RedisReply(_: ByteString).asList.flatten // TODO remove intermediate Option
  }

  case class RandomKey[A](implicit parse: Parse[A]) extends KeyCommand {
    type Ret = Option[A]
    val line = multiBulk(Seq("RANDOMKEY"))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }

  case class Rename(oldKey: Any, newKey: Any, nx: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (nx) "RENAMENX" else "RENAME") +: (Seq(oldKey, newKey) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object DBSize extends KeyCommand {
    type Ret = Long
    val line = multiBulk(Seq("DBSIZE"))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class Exists(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("EXISTS" +: Seq(format.apply(key)))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class Del(key: Any, keys: Any*)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    val line = multiBulk("DEL" +: ((key :: keys.toList) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class GetType(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = String
    val line = multiBulk("TYPE" +: Seq(format.apply(key)))
    val ret  = RedisReply(_: ByteString).asString
  }

  case class Expire(key: Any, ttl: Int, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (millis) "PEXPIRE" else "EXPIRE") +: (Seq(key, ttl) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class ExpireAt(key: Any, timestamp: Long, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (millis) "PEXPIREAT" else "EXPIREAT") +: (Seq(key, timestamp) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class TTL(key: Any, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    val line = multiBulk((if (millis) "PTTL" else "TTL") +: Seq(format.apply(key)))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class FlushDB(all: Boolean = false) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk(Seq(if (all) "FLUSHALL" else "FLUSHDB"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class Move(key: Any, db: Int)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("MOVE" +: (Seq(key, db) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case object Quit extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("QUIT"))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class Auth(secret: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("AUTH" +: (Seq(secret) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class Persist(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("PERSIST" +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  // case class Select ..
}
