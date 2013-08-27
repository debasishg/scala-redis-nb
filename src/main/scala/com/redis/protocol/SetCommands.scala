package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object SetCommands {

  case class SAdd(key: String, values: Seq[Stringified]) extends RedisCommand[Long] {
    require(values.nonEmpty)
    def line = multiBulk("SADD" +: key +: values.map(_.value))
  }

  object SAdd {
    def apply(key: String, value: Stringified, values: Stringified*): SAdd = SAdd(key, value +: values)
  }


  case class SRem(key: String, values: Seq[Stringified]) extends RedisCommand[Long] {
    require(values.nonEmpty)
    def line = multiBulk("SREM" +: key +: values.map(_.value))
  }

  object SRem {
    def apply(key: String, value: Stringified, values: Stringified*): SRem = SRem(key, value +: values)
  }


  case class SPop[A](key: String)(implicit reader: Reader[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SPOP" +: Seq(key))
  }
  
  case class SMove(srcKey: String, destKey: String, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("SMOVE" +: Seq(srcKey, destKey, value.value))
  }

  case class SCard(key: String) extends RedisCommand[Long] {
    def line = multiBulk("SCARD" +: Seq(key))
  }

  case class SIsMember(key: String, value: Stringified) extends RedisCommand[Boolean] {
    def line = multiBulk("SISMEMBER" +: Seq(key, value.value))
  }


  case class SInter[A](keys: Seq[String])(implicit reader: Reader[A]) extends RedisCommand[Set[A]] {
    require(keys.nonEmpty)
    def line = multiBulk("SINTER" +: keys)
  }

  object SInter {
    def apply[A](key: String, keys: String*)(implicit reader: Reader[A]): SInter[A] = SInter(key +: keys)
  }

  
  case class SUnion[A](keys: Seq[String])(implicit reader: Reader[A]) extends RedisCommand[Set[A]] {
    require(keys.nonEmpty)
    def line = multiBulk("SUNION" +: keys)
  }

  object SUnion {
    def apply[A](key: String, keys: String*)(implicit reader: Reader[A]): SUnion[A] = SUnion(key +: keys)
  }


  case class SDiff[A](keys: Seq[String])(implicit reader: Reader[A]) extends RedisCommand[Set[A]] {
    require(keys.nonEmpty)
    def line = multiBulk("SDIFF" +: keys)
  }

  object SDiff {
    def apply[A](key: String, keys: String*)(implicit reader: Reader[A]): SDiff[A] = SDiff(key +: keys)
  }


  case class SInterStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long] {
    require(keys.nonEmpty)
    def line = multiBulk("SINTERSTORE" +: destKey +: keys)
  }

  object SInterStore {
    def apply(destKey: String, key: String, keys: String*): SInterStore =
      SInterStore(destKey, key +: keys)
  }


  case class SUnionStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long] {
    require(keys.nonEmpty)
    def line = multiBulk("SUNIONSTORE" +: destKey +: keys)
  }

  object SUnionStore {
    def apply(destKey: String, key: String, keys: String*): SUnionStore =
      SUnionStore(destKey, key +: keys)
  }


  case class SDiffStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long] {
    require(keys.nonEmpty)
    def line = multiBulk("SDIFFSTORE" +: destKey +: keys)
  }

  object SDiffStore {
    def apply(destKey: String, key: String, keys: String*): SDiffStore =
      SDiffStore(destKey, key +: keys)
  }


  case class SMembers[A](key: String)(implicit reader: Reader[A]) extends RedisCommand[Set[A]] {
    def line = multiBulk("SMEMBERS" +: Seq(key))
  }

  case class SRandMember[A](key: String)(implicit reader: Reader[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("SRANDMEMBER" +: Seq(key))
  }

  case class SRandMembers[A](key: String, count: Int)(implicit reader: Reader[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("SRANDMEMBER" +: (Seq(key, count.toString)))
  }
}
