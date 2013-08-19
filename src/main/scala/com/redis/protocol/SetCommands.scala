package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object SetCommands {

  trait Op
  case object add extends Op
  case object rem extends Op

  case class SOp[A](op: Op, key: String, value: A, values: A*)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk((if (op == add) "SADD" else "SREM") +: key +: (value +: values).map(writer.write))
  }

  case class SPop[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SPOP" +: Seq(key))
  }
  
  case class SMove[A](srcKey: String, destKey: String, value: A)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("SMOVE" +: Seq(srcKey, destKey, writer.write(value)))
  }

  case class SCard(key: String) extends RedisCommand[Long] {
    def line = multiBulk("SCARD" +: Seq(key))
  }

  case class ∈[A](key: String, value: A)(implicit writer: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("SISMEMBER" +: Seq(key, writer.write(value)))
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp
  case object diff extends setOp

  case class ∩∪∖[A](ux: setOp, key: String, keys: String*)(implicit reader: Read[A]) extends RedisCommand[Set[A]] {
    def line = multiBulk(
      (if (ux == inter) "SINTER" else if (ux == union) "SUNION" else "SDIFF") +: key +: keys)
  }
  
  case class SUXDStore(ux: setOp, destKey: String, key: String, keys: String*) extends RedisCommand[Long] {
    def line = multiBulk(
      (if (ux == inter) "SINTERSTORE" else if (ux == union) "SUNIONSTORE" else "SDIFFSTORE") +: destKey +: key +: keys)
  }

  case class SMembers[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Set[A]] {
    def line = multiBulk("SDIFF" +: Seq(key))
  }

  case class SRandMember[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SRANDMEMBER" +: Seq(key))
  }

  case class SRandMembers[A](key: String, count: Int)(implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("SRANDMEMBER" +: (Seq(key, count.toString)))
  }
}
