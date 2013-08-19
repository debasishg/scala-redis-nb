package com.redis.protocol

import com.redis.serialization.{PartialDeserializer, Read, Write}
import RedisCommand._


object HashCommands {
  case class HSet[A](key: String, field: String, value: A, nx: Boolean = false)
                    (implicit writer: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk(
      (if (nx) "HSETNX" else "HSET") +: Seq(key, field, writer.write(value)))
  }
  
  case class HGet[A](key: String, field: String)
                    (implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("HGET" +: Seq(key, field))
  }
  
  case class HMSet[A](key: String, mapLike: Iterable[Product2[String, A]])
                  (implicit writer: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("HMSET" +: key +: flattenPairs(mapLike))
  }
  
  case class HMGet[A](key: String, fields: String*)(implicit reader: Read[A])
      extends RedisCommand[Map[String, A]]()(PartialDeserializer.keyedMapPD(fields)) {
    def line = multiBulk("HMGET" +: key +: fields)
  }
  
  case class HIncrby(key: String, field: String, value: Int) extends RedisCommand[Long] {
    def line = multiBulk("HINCRBY" +: Seq(key, field, value.toString))
  }
  
  case class HExists(key: String, field: String) extends RedisCommand[Boolean] {
    def line = multiBulk("HEXISTS" +: Seq(key, field))
  }
  
  case class HDel(key: String, field: String, fields: String*) extends RedisCommand[Long] {
    def line = multiBulk("HDEL" +: key +: field +: fields)
  }
  
  case class HLen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("HLEN" +: Seq(key))
  }
  
  case class HKeys(key: String) extends RedisCommand[List[String]] {
    def line = multiBulk("HKEYS" +: Seq(key))
  }
  
  case class HVals[A](key: String)(implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("HVALS" +: Seq(key))
  }
  
  case class HGetall[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Map[String, A]] {
    def line = multiBulk("HGETALL" +: Seq(key))
  }
}
