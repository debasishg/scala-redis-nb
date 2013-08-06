package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.HashCommands

trait HashOperations { this: RedisOps =>
  import HashCommands._

  def hset(key: Any, field: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HSet(key, field, value)).mapTo[HSet#Ret]

  def hsetnx(key: Any, field: Any, value: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HSet(key, field, value, true)).mapTo[HSet#Ret]

  def hget[A](key: Any, field: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HGet(key, field)).mapTo[HGet[A]#Ret]

  def hmset(key: Any, map: Iterable[Product2[Any,Any]])(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HMSet(key, map)).mapTo[HMSet#Ret]

  def hmget[K, V](key: Any, fields: K*)(implicit timeout: Timeout, format: Format, parseV: Parse[V]) =
    clientRef.ask(HMGet(key, fields:_*)).mapTo[HMGet[K, V]#Ret]

  def hincrby(key: Any, field: Any, value: Int)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HIncrby(key, field, value)).mapTo[HIncrby#Ret]

  def hexists(key: Any, field: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HExists(key, field)).mapTo[HExists#Ret]

  def hdel(key: Any, field: Any, fields: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HDel(key, field, fields:_*)).mapTo[HDel#Ret]

  def hlen(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(HLen(key)).mapTo[HLen#Ret]

  def hkeys[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HKeys(key)).mapTo[HKeys[A]#Ret]

  def hvals[A](key: Any)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(HVals(key)).mapTo[HVals[A]#Ret]

  def hgetall[K, V](key: Any)(implicit timeout: Timeout, format: Format, parseK: Parse[K], parseV: Parse[V]) =
    clientRef.ask(HGetall(key)).mapTo[HGetall[K, V]#Ret]
}
