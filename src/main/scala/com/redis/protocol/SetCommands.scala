package com.redis.protocol

import com.redis.serialization._


object SetCommands {
  import DefaultWriters._

  case class SAdd(key: String, values: Seq[Stringified]) extends RedisCommand[Long]("SADD") {
    require(values.nonEmpty)
    def params = key +: values.toArgs
  }

  object SAdd {
    def apply(key: String, value: Stringified, values: Stringified*): SAdd = SAdd(key, value +: values)
  }


  case class SRem(key: String, values: Seq[Stringified]) extends RedisCommand[Long]("SREM") {
    require(values.nonEmpty)
    def params = key +: values.toArgs
  }

  object SRem {
    def apply(key: String, value: Stringified, values: Stringified*): SRem = SRem(key, value +: values)
  }


  case class SPop[A: Reader](key: String) extends RedisCommand[Option[A]]("SPOP") {
    def params = key +: ANil
  }
  
  case class SMove(srcKey: String, destKey: String, value: Stringified) extends RedisCommand[Long]("SMOVE") {
    def params = srcKey +: destKey +: value +: ANil
  }

  case class SCard(key: String) extends RedisCommand[Long]("SCARD") {
    def params = key +: ANil
  }

  case class SIsMember(key: String, value: Stringified) extends RedisCommand[Boolean]("SISMEMBER") {
    def params = key +: value +: ANil
  }


  case class SInter[A: Reader](keys: Seq[String]) extends RedisCommand[Set[A]]("SINTER") {
    require(keys.nonEmpty)
    def params = keys.toArgs
  }

  object SInter {
    def apply[A: Reader](key: String, keys: String*): SInter[A] = SInter(key +: keys)
  }

  
  case class SUnion[A: Reader](keys: Seq[String]) extends RedisCommand[Set[A]]("SUNION") {
    require(keys.nonEmpty)
    def params = keys.toArgs
  }

  object SUnion {
    def apply[A: Reader](key: String, keys: String*): SUnion[A] = SUnion(key +: keys)
  }


  case class SDiff[A: Reader](keys: Seq[String]) extends RedisCommand[Set[A]]("SDIFF") {
    require(keys.nonEmpty)
    def params = keys.toArgs
  }

  object SDiff {
    def apply[A: Reader](key: String, keys: String*): SDiff[A] = SDiff(key +: keys)
  }


  case class SInterStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long]("SINTERSTORE") {
    require(keys.nonEmpty)
    def params = destKey +: keys.toArgs
  }

  object SInterStore {
    def apply(destKey: String, key: String, keys: String*): SInterStore =
      SInterStore(destKey, key +: keys)
  }


  case class SUnionStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long]("SUNIONSTORE") {
    require(keys.nonEmpty)
    def params = destKey +: keys.toArgs
  }

  object SUnionStore {
    def apply(destKey: String, key: String, keys: String*): SUnionStore =
      SUnionStore(destKey, key +: keys)
  }


  case class SDiffStore(destKey: String, keys: Seq[String]) extends RedisCommand[Long]("SDIFFSTORE") {
    require(keys.nonEmpty)
    def params = destKey +: keys.toArgs
  }

  object SDiffStore {
    def apply(destKey: String, key: String, keys: String*): SDiffStore =
      SDiffStore(destKey, key +: keys)
  }


  case class SMembers[A: Reader](key: String) extends RedisCommand[Set[A]]("SMEMBERS") {
    def params = key +: ANil
  }

  case class SRandMember[A: Reader](key: String) extends RedisCommand[Option[A]]("SRANDMEMBER") {
    def params = key +: ANil
  }

  case class SRandMembers[A: Reader](key: String, count: Int) extends RedisCommand[List[A]]("SRANDMEMBER") {
    def params = key +: count +: ANil
  }

}
