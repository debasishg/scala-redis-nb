package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import akka.util.ByteString


object SetCommands {

  trait Op
  case object add extends Op
  case object rem extends Op

  case class SOp(op: Op, key: Any, value: Any, values: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk((if (op == add) "SADD" else "SREM") +: (key :: value :: values.toList) map format.apply)
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class SPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Option[A]
    val line = multiBulk("SPOP" +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }
  
  case class SMove(srcKey: Any, destKey: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk("SMOVE" +: (Seq(srcKey, destKey, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class SCard(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk("SCARD" +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class ∈(key: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Boolean
    val line = multiBulk("SISMEMBER" +: (Seq(key, value) map format.apply))
    val ret  = RedisReply(_: ByteString).asBoolean
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp
  case object diff extends setOp

  case class ∩∪∖[A](ux: setOp, key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Set[A]
    val line = multiBulk(
      (if (ux == inter) "SINTER" else if (ux == union) "SUNION" else "SDIFF") +: (key :: keys.toList) map format.apply)
    val ret  = RedisReply(_: ByteString).asSet[A]
  }
  
  case class SUXDStore(ux: setOp, destKey: Any, key: Any, keys: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    val line = multiBulk(
      (if (ux == inter) "SINTERSTORE" else if (ux == union) "SUNIONSTORE" else "SDIFFSTORE") +: (destKey :: key :: keys.toList) map format.apply)
    val ret  = RedisReply(_: ByteString).asLong
  }

  case class SMembers[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Set[A]
    val line = multiBulk("SDIFF" +: Seq(key) map format.apply)
    val ret  = RedisReply(_: ByteString).asSet[A]
  }

  case class SRandMember[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Option[A]
    val line = multiBulk("SRANDMEMBER" +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: ByteString).asBulk[A]
  }

  case class SRandMembers[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = List[A]
    val line = multiBulk("SRANDMEMBER" +: (Seq(key, count) map format.apply))
    val ret  = RedisReply(_: ByteString).asList[A].flatten // TODO remove intermediate Option
  }
}
