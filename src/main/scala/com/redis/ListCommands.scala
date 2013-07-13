package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._

object ListCommands {
  case class LPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("LPUSH".getBytes("UTF-8") +: (key :: value :: values.toList) map format.apply)
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class LPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("LPUSHX".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class RPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("RPUSH".getBytes("UTF-8") +: (key :: value :: values.toList) map format.apply)
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class RPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("RPUSHX".getBytes("UTF-8") +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[List[Option[A]]]
    val line = multiBulk("LRANGE".getBytes("UTF-8") +: (Seq(key, start, stop) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asList
  }

  case class LLen(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("LLEN".getBytes("UTF-8") +: (Seq(format.apply(key))))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class LTrim(key: Any, start: Int, end: Int)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LTRIM".getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }
  
  case class LIndex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("LINDEX".getBytes("UTF-8") +: (Seq(key, index) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }

  case class LSet(key: Any, index: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LSET".getBytes("UTF-8") +: (Seq(key, index, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBoolean
  }

  case class LRem(key: Any, count: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Option[Long]
    val line = multiBulk("LREM".getBytes("UTF-8") +: (Seq(key, count, value) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class LPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("LPOP".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }
  
  case class RPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("RPOP".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }
  
  case class RPopLPush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("RPOPLPUSH".getBytes("UTF-8") +: (Seq(srcKey, dstKey) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }
  
  case class BRPopLPush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("BRPOPLPUSH".getBytes("UTF-8") +: (Seq(srcKey, dstKey, timeoutInSeconds) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk[A]
  }
  
  case class BLPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = Option[(K, V)]
    val line = multiBulk("BLPOP".getBytes("UTF-8") +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asListPairs[K,V].flatMap(_.flatten.headOption)
  }
  
  case class BRPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = Option[(K, V)]
    val line = multiBulk("BRPOP".getBytes("UTF-8") +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asListPairs[K,V].flatMap(_.flatten.headOption)
  }
}
