package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._

object HashCommands {
  case class HSet(key: Any, field: Any, value: Any, nx: Boolean = false)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk(
      (if (nx) "HSETNX" else "HSET").getBytes("UTF-8") +: (List(key, field, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }
  
  case class HGet[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = Option[A]
    val line = multiBulk("HGET".getBytes("UTF-8") +: (List(key, field) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }
  
  case class HMSet(key: Any, map: Iterable[Product2[Any,Any]])(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HMSET".getBytes("UTF-8") +: ((key :: flattenPairs(map)) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }
  
  case class HMGet[K,V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) extends HashCommand {
    type Ret = Map[K, V]
    val line = multiBulk("HMGET".getBytes("UTF-8") +: ((key :: fields.toList) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asList.zip(fields).collect {
      case (Some(value), field) => (field, value)
    }.toMap
  }
  
  case class HIncrby(key: Any, field: Any, value: Int)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HINCRBY".getBytes("UTF-8") +: (List(key, field, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class HExists(key: Any, field: Any)(implicit format: Format) extends HashCommand {
    type Ret = Boolean
    val line = multiBulk("HEXISTS".getBytes("UTF-8") +: (List(key, field) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }
  
  case class HDel(key: Any, field: Any, fields: Any*)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HDEL".getBytes("UTF-8") +: ((key :: field :: fields.toList) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class HLen(key: Any)(implicit format: Format) extends HashCommand {
    type Ret = Long
    val line = multiBulk("HLEN".getBytes("UTF-8") +: (List(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class HKeys[A](key: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = List[A]
    val line = multiBulk("HKEYS".getBytes("UTF-8") +: (List(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asList.flatten // TODO remove intermediate Option
  }
  
  case class HVals[A](key: Any)(implicit format: Format, parse: Parse[A]) extends HashCommand {
    type Ret = List[A]
    val line = multiBulk("HVALS".getBytes("UTF-8") +: (List(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asList.flatten // TODO remove intermediate Option
  }
  
  case class HGetall[K,V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends HashCommand {
    type Ret = Map[K, V]
    val line = multiBulk("HGETALL".getBytes("UTF-8") +: (List(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asListPairs[K,V].flatten.toMap // TODO remove intermediate Option
  }
}
