package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import akka.pattern.ask
import akka.actor._
import RedisCommand._
import RedisReplies._
import akka.util.ByteString


object StringCommands {
  case class Get[A](key: Any)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = Option[A]
    val line = multiBulk("GET" +: Seq(format.apply(key)))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }
  
  sealed trait SetExpiryOption
  case class EX(expiryInSeconds: Long) extends SetExpiryOption
  case class PX(expiryInMillis: Long) extends SetExpiryOption

  sealed trait SetConditionOption
  case object NX extends SetConditionOption
  case object XX extends SetConditionOption

  case class Set(key: Any, value: Any, nxORxx: Option[SetConditionOption] = None, exORpx: Option[SetExpiryOption] = None)
    (implicit format: Format) extends StringCommand {

    type Ret = Boolean
    val line = multiBulk("SET" +: (Seq(key, value) ++
      ((nxORxx, exORpx) match {
        case (Some(NX), Some(EX(n))) => Seq("EX", n, "NX")
        case (Some(XX), Some(EX(n))) => Seq("EX", n, "XX")
        case (Some(NX), Some(PX(n))) => Seq("PX", n, "NX")
        case (Some(XX), Some(PX(n))) => Seq("PX", n, "XX")
        case (None, Some(EX(n))) => Seq("EX", n)
        case (None, Some(PX(n))) => Seq("PX", n)
        case (Some(NX), None) => Seq("NX")
        case (Some(XX), None) => Seq("XX")
        case (None, None) => Seq.empty
      })) map format.apply)
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class GetSet[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = Option[A]
    val line = multiBulk("GETSET" +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }
  
  case class SetNx(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("SETNX" +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class SetEx(key: Any, expiry: Long, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("SETEX" +: (Seq(key, expiry, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class PSetEx(key: Any, expiryInMillis: Long, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("PSETEX" +: (Seq(key, expiryInMillis, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class Incr(key: Any, by: Option[Int] = None)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk(
      by.map(i => "INCRBY" +: (Seq(key, i) map format.apply))
        .getOrElse("INCR" +: (Seq(format.apply(key))))
    )
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class Decr(key: Any, by: Option[Int] = None)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk(
      by.map(i => "DECRBY" +: (Seq(key, i) map format.apply))
        .getOrElse("DECR" +: (Seq(format.apply(key))))
    )
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class MGet[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = List[Option[A]]
    val line = multiBulk("MGET" +: ((key :: keys.toList) map format.apply))
    val ret  = RedisReply(_: ByteString).asList[A]
  }

  case class MSet(kvs: (Any, Any)*)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("MSET" +: (kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l }) map format.apply)
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  case class MSetNx(kvs: (Any, Any)*)(implicit format: Format) extends StringCommand {
    type Ret = Boolean
    val line = multiBulk("MSETNX" +: (kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l }) map format.apply)
    val ret  = RedisReply(_: ByteString).asBoolean
  }
  
  case class SetRange(key: Any, offset: Int, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("SETRANGE" +: (Seq(key, offset, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class GetRange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]) extends StringCommand {
    type Ret = Option[A]
    val line = multiBulk("GETRANGE" +: (Seq(key, start, end) map format.apply))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }
  
  case class Strlen(key: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("STRLEN" +: Seq(format.apply(key)))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class Append(key: Any, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("APPEND" +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class GetBit(key: Any, offset: Int)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("GETBIT" +: (Seq(key, offset) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class SetBit(key: Any, offset: Int, value: Any)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("SETBIT" +: (Seq(key, offset, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class BitOp(op: String, destKey: Any, srcKeys: Any*)(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("BITOP" +: ((op :: destKey :: srcKeys.toList) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }
  
  case class BitCount(key: Any, range: Option[(Int, Int)])(implicit format: Format) extends StringCommand {
    type Ret = Long
    val line = multiBulk("BITCOUNT" +: (List(key) ++ (range.map(_.productIterator.toList).getOrElse(List()))) map format.apply)
    val ret  = RedisReply(_: ByteString).asLong
  }
}

