package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import RedisCommand._

trait HashOperations { this: RedisOps =>
  import HashCommands._

  def hset(key: Any, field: Any, value: Any)(implicit format: Format) =
    actorRef.ask(HSet(key, field, value)).mapTo[Boolean]

  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format) =
    actorRef.ask(HSet(key, field, value, true)).mapTo[Boolean]

  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]) =
    actorRef.ask(HGet(key, field)).mapTo[Option[A]]

  def hmset(key: Any, map: Iterable[Product2[Any,Any]])(implicit format: Format) =
    actorRef.ask(HMSet(key, map)).mapTo[Boolean]

  def hmget[K,V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]) =
    actorRef.ask(HMGet(key, fields:_*)).mapTo[Map[K, V]]

  def hincrby(key: Any, field: Any, value: Int)(implicit format: Format) =
    actorRef.ask(HIncrby(key, field, value)).mapTo[Long]

  def hexists(key: Any, field: Any)(implicit format: Format) =
    actorRef.ask(HExists(key, field)).mapTo[Boolean]

  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format) =
    actorRef.ask(HDel(key, field, fields:_*)).mapTo[Long]

  def hlen(key: Any)(implicit format: Format) =
    actorRef.ask(HLen(key)).mapTo[Long]

  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]) =
    actorRef.ask(HKeys(key)).mapTo[List[A]]

  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]) =
    actorRef.ask(HVals(key)).mapTo[List[A]]

  def hgetall[K,V](key: Any)(implicit format: Format, parseK: Parse[K], parseV: Parse[V]) =
    actorRef.ask(HGetall(key)).mapTo[Map[K, V]]
}
