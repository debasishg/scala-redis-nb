package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object SetCommands {

  trait Op
  case object add extends Op
  case object rem extends Op

  case class SOp[A](op: Op, key: String, value: A, values: A*)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk((if (op == add) "SADD" else "SREM") :: key :: (value :: values.toList).map(write))
  }

  case class SPop[A](key: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SPOP" :: List(key))
  }
  
  case class SMove[A](srcKey: String, destKey: String, value: A)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("SMOVE" :: List(srcKey, destKey, write(value)))
  }

  case class SCard(key: String) extends RedisCommand[Long] {
    def line = multiBulk("SCARD" :: List(key))
  }

  case class ∈[A](key: String, value: A)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("SISMEMBER" :: List(key, write(value)))
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp
  case object diff extends setOp

  case class ∩∪∖[A](ux: setOp, key: String, keys: String*)(implicit read: Read[A]) extends RedisCommand[Set[A]] {
    def line = multiBulk(
      (if (ux == inter) "SINTER" else if (ux == union) "SUNION" else "SDIFF") :: (key :: keys.toList))
  }
  
  case class SUXDStore(ux: setOp, destKey: String, key: String, keys: String*) extends RedisCommand[Long] {
    def line = multiBulk(
      (if (ux == inter) "SINTERSTORE" else if (ux == union) "SUNIONSTORE" else "SDIFFSTORE") :: (destKey :: key :: keys.toList))
  }

  case class SMembers[A](key: String)(implicit read: Read[A]) extends RedisCommand[Set[A]] {
    def line = multiBulk("SDIFF" :: List(key))
  }

  case class SRandMember[A](key: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SRANDMEMBER" :: (List(key)))
  }

  case class SRandMembers[A](key: String, count: Int)(implicit read: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("SRANDMEMBER" :: (List(key, count.toString)))
  }
}
