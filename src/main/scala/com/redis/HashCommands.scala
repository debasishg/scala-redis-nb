package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._
import akka.util.ByteString


object HashCommands {
  case class HSet(key: Any, field: Any, value: Any, nx: Boolean = false)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk(
      (if (nx) "HSETNX" else "HSET") +: (List(key, field, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class HGet[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = Option[A]
    val line = multiBulk("HGET" +: (List(key, field) map format.apply))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }
  
  case class HMSet(key: Any, map: Iterable[Product2[Any,Any]])(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HMSET" +: ((key :: flattenPairs(map)) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class HMGet[K,V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) extends HashCommand {
    type Ret = Map[K, V]
    val line = multiBulk("HMGET" +: ((key :: fields.toList) map format.apply))
    val ret  = RedisReply(_: ByteString).asList.zip(fields).collect {
      case (Some(value), field) => (field, value)
    }.toMap
  }
  
  case class HIncrby(key: Any, field: Any, value: Int)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HINCRBY" +: (List(key, field, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class HExists(key: Any, field: Any)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HEXISTS" +: (List(key, field) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class HDel(key: Any, field: Any, fields: Any*)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HDEL" +: ((key :: field :: fields.toList) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class HLen(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HLEN" +: (List(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class HKeys[A](key: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = List[A]
    val line = multiBulk("HKEYS" +: (List(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asList.flatten // TODO remove intermediate Option
  }
  
  case class HVals[A](key: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = List[A]
    val line = multiBulk("HVALS" +: (List(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asList.flatten // TODO remove intermediate Option
  }
  
  case class HGetall[K,V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends HashCommand {
    type Ret = Map[K, V]
    val line = multiBulk("HGETALL" +: (List(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asListPairs[K,V].flatten.toMap // TODO remove intermediate Option
  }
}
