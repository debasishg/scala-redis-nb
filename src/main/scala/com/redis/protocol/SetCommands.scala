package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object SetCommands {

  trait Op
  case object add extends Op
  case object rem extends Op

  case class SOp(op: Op, key: Any, value: Any, values: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    def line = multiBulk((if (op == add) "SADD" else "SREM") +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Option[A]
    def line = multiBulk("SPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }
  
  case class SMove(srcKey: Any, destKey: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    def line = multiBulk("SMOVE" +: (Seq(srcKey, destKey, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SCard(key: Any)(implicit format: Format) extends SetCommand {
    type Ret = Long
    def line = multiBulk("SCARD" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ∈(key: Any, value: Any)(implicit format: Format) extends SetCommand {
    type Ret = Boolean
    def line = multiBulk("SISMEMBER" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp
  case object diff extends setOp

  case class ∩∪∖[A](ux: setOp, key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Set[A]
    def line = multiBulk(
      (if (ux == inter) "SINTER" else if (ux == union) "SUNION" else "SDIFF") +: (key :: keys.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asSet[A]
  }
  
  case class SUXDStore(ux: setOp, destKey: Any, key: Any, keys: Any*)(implicit format: Format) extends SetCommand {
    type Ret = Long
    def line = multiBulk(
      (if (ux == inter) "SINTERSTORE" else if (ux == union) "SUNIONSTORE" else "SDIFFSTORE") +: (destKey :: key :: keys.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class SMembers[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Set[A]
    def line = multiBulk("SDIFF" +: Seq(key) map format.apply)
    val ret  = (_: RedisReply[_]).asSet[A]
  }

  case class SRandMember[A](key: Any)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = Option[A]
    def line = multiBulk("SRANDMEMBER" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }

  case class SRandMembers[A](key: Any, count: Int)(implicit format: Format, parse: Parse[A]) extends SetCommand {
    type Ret = List[A]
    def line = multiBulk("SRANDMEMBER" +: (Seq(key, count) map format.apply))
    val ret  = (_: RedisReply[_]).asList[A].flatten // TODO remove intermediate Option
  }
}
