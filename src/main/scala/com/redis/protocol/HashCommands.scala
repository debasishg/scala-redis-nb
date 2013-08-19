package com.redis.protocol

import com.redis.serialization.{PartialDeserializer, Read, Write}
import RedisCommand._


object HashCommands {
  case class HSet[A](key: String, field: String, value: A, nx: Boolean = false)
                    (implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk(
      (if (nx) "HSETNX" else "HSET") :: List(key, field, write(value)))
  }
  
  case class HGet[A](key: String, field: String)
                    (implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("HGET" :: List(key, field))
  }
  
  case class HMSet[A](key: String, mapLike: Iterable[Product2[String, A]])
                  (implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("HMSET" :: key :: flattenPairs(mapLike))
  }
  
  case class HMGet[A](key: String, fields: String*)(implicit read: Read[A])
      extends RedisCommand[Map[String, A]]()(PartialDeserializer.keyedMapPD(fields)) {
    def line = multiBulk("HMGET" :: key :: fields.toList)
  }
  
  case class HIncrby(key: String, field: String, value: Int) extends RedisCommand[Long] {
    def line = multiBulk("HINCRBY" :: List(key, field, value.toString))
  }
  
  case class HExists(key: String, field: String) extends RedisCommand[Boolean] {
    def line = multiBulk("HEXISTS" :: List(key, field))
  }
  
  case class HDel(key: String, field: String, fields: String*) extends RedisCommand[Long] {
    def line = multiBulk("HDEL" :: key :: field :: fields.toList)
  }
  
  case class HLen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("HLEN" :: List(key))
  }
  
  case class HKeys[A](key: String)(implicit read: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("HKEYS" :: List(key))
  }
  
  case class HVals[A](key: String)(implicit read: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("HVALS" :: List(key))
  }
  
  case class HGetall[A](key: String)(implicit read: Read[A]) extends RedisCommand[Map[String, A]] {
    def line = multiBulk("HGETALL" :: List(key))
  }
}
