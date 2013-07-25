package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.command.HashCommands

trait HashOperations { this: RedisOps =>
  import HashCommands._

  def hset(key: Any, field: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HSet(key, field, value)).mapTo[Boolean]

  def hsetnx(key: Any, field: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HSet(key, field, value, true)).mapTo[Boolean]

  def hget[A](key: Any, field: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HGet(key, field)).mapTo[Option[A]]

  def hmset(key: Any, map: Iterable[Product2[Any,Any]])(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HMSet(key, map)).mapTo[Boolean]

  def hmget[K,V](key: Any, fields: K*)(implicit timeout: Timeout, format: Format, parseV: Parse[V]) =
    clientRef.ask(HMGet(key, fields:_*)).mapTo[Map[K, V]]

  def hincrby(key: Any, field: Any, value: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HIncrby(key, field, value)).mapTo[Long]

  def hexists(key: Any, field: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HExists(key, field)).mapTo[Boolean]

  def hdel(key: Any, field: Any, fields: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HDel(key, field, fields:_*)).mapTo[Long]

  def hlen(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HLen(key)).mapTo[Long]

  def hkeys[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HKeys(key)).mapTo[List[A]]

  def hvals[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HVals(key)).mapTo[List[A]]

  def hgetall[K,V](key: Any)(implicit timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(HGetall(key)).mapTo[Map[K, V]]
}
