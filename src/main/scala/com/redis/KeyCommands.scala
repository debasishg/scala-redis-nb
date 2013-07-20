package com.redis

import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._

object KeyCommands {
  case class Keys[A](pattern: Any = "*")(implicit format: Format, parse: Parse[A]) extends KeyCommand {
    type Ret = List[A]
    val line = multiBulk("KEYS".getBytes("UTF-8") +: Seq(format.apply(pattern)))
    val ret  = RedisReply(_: Array[Byte]).asList.flatten // TODO remove intermediate Option
  }

  case class RandomKey[A](implicit parse: Parse[A]) extends KeyCommand {
    type Ret = Option[A]
    val line = multiBulk(Seq("RANDOMKEY".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }

  case class Rename(oldKey: Any, newKey: Any, nx: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (nx) "RENAMENX" else "RENAME").getBytes("UTF-8") +: (Seq(oldKey, newKey) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object DBSize extends KeyCommand {
    type Ret = Long
    val line = multiBulk(Seq("DBSIZE".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class Exists(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("EXISTS".getBytes("UTF-8") +: Seq(format.apply(key)))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class Del(key: Any, keys: Any*)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    val line = multiBulk("DEL".getBytes("UTF-8") +: ((key :: keys.toList) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class GetType(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = String
    val line = multiBulk("TYPE".getBytes("UTF-8") +: Seq(format.apply(key)))
    val ret  = RedisReply(_: Array[Byte]).asString
  }

  case class Expire(key: Any, ttl: Int, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (millis) "PEXPIRE" else "EXPIRE").getBytes("UTF-8") +: (Seq(key, ttl) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class ExpireAt(key: Any, timestamp: Long, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk((if (millis) "PEXPIREAT" else "EXPIREAT").getBytes("UTF-8") +: (Seq(key, timestamp) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class TTL(key: Any, millis: Boolean = false)(implicit format: Format) extends KeyCommand {
    type Ret = Long
    val line = multiBulk((if (millis) "PTTL" else "TTL").getBytes("UTF-8") +: Seq(format.apply(key)))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class FlushDB(all: Boolean = false) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk(Seq((if (all) "FLUSHALL" else "FLUSHDB").getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class Move(key: Any, db: Int)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("MOVE".getBytes("UTF-8") +: (Seq(key, db) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case object Quit extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk(Seq("QUIT".getBytes("UTF-8")))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class Auth(secret: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("AUTH".getBytes("UTF-8") +: (Seq(secret) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class Persist(key: Any)(implicit format: Format) extends KeyCommand {
    type Ret = Boolean
    val line = multiBulk("PERSIST".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  // case class Select ..
}
