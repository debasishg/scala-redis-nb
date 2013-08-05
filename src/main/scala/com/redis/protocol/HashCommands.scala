package com.redis.protocol

import com.redis.serialization.{PartialDeserializer, Parse, Format}
import RedisCommand._


object HashCommands {
  case class HSet(key: Any, field: Any, value: Any, nx: Boolean = false)
                 (implicit format: Format) extends RedisCommand[Boolean] {
    def line = multiBulk(
      (if (nx) "HSETNX" else "HSET") +: (List(key, field, value) map format.apply))
  }
  
  case class HGet[A](key: Any, field: Any)
                    (implicit format: Format, parse: Parse[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("HGET" +: (List(key, field) map format.apply))
  }
  
  case class HMSet(key: Any, map: Iterable[Product2[Any,Any]])
                  (implicit format: Format) extends RedisCommand[Boolean] {
    def line = multiBulk("HMSET" +: ((key :: flattenPairs(map)) map format.apply))
  }
  
  case class HMGet[K,V](key: Any, fields: K*)
                       (implicit format: Format, parseV: Parse[V]) extends RedisCommand[Map[K, V]]()(
    PartialDeserializer(PartialDeserializer.listOptPD[V] andThen (_.zip(fields).collect {
      case (Some(value), field) => (field, value)
    }.toMap))
  ) {
    def line = multiBulk("HMGET" +: ((key :: fields.toList) map format.apply))
  }
  
  case class HIncrby(key: Any, field: Any, value: Int)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("HINCRBY" +: (List(key, field, value) map format.apply))
  }
  
  case class HExists(key: Any, field: Any)(implicit format: Format) extends RedisCommand[Boolean] {
    def line = multiBulk("HEXISTS" +: (List(key, field) map format.apply))
  }
  
  case class HDel(key: Any, field: Any, fields: Any*)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("HDEL" +: ((key :: field :: fields.toList) map format.apply))
  }
  
  case class HLen(key: Any)(implicit format: Format) extends RedisCommand[Long] {
    def line = multiBulk("HLEN" +: (List(key) map format.apply))
  }
  
  case class HKeys[A](key: Any)(implicit format: Format, parse: Parse[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("HKEYS" +: (List(key) map format.apply))
  }
  
  case class HVals[A](key: Any)(implicit format: Format, parse: Parse[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("HVALS" +: (List(key) map format.apply))
  }
  
  case class HGetall[K,V](key: Any)
                         (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends RedisCommand[Map[K, V]] {
    def line = multiBulk("HGETALL" +: (List(key) map format.apply))
  }
}
