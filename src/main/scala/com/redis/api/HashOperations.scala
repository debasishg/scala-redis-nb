package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.HashCommands

trait HashOperations { this: RedisOps =>
  import HashCommands._

  def hset[A](key: String, field: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(HSet[A](key, field, value)).mapTo[HSet[A]#Ret]

  def hsetnx[A](key: String, field: String, value: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(HSet[A](key, field, value, true)).mapTo[HSet[A]#Ret]

  def hget[A](key: String, field: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(HGet[A](key, field)).mapTo[HGet[A]#Ret]

  def hmset[A](key: String, mapLike: Iterable[Product2[String, A]])(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(HMSet[A](key, mapLike)).mapTo[HMSet[A]#Ret]

  def hmget[A](key: String, fields: String*)(implicit timeout: Timeout, parseV: Read[A]) =
    clientRef.ask(HMGet[A](key, fields:_*)).mapTo[HMGet[A]#Ret]

  def hincrby(key: String, field: String, value: Int)(implicit timeout: Timeout) =
    clientRef.ask(HIncrby(key, field, value)).mapTo[HIncrby#Ret]

  def hexists(key: String, field: String)(implicit timeout: Timeout) =
    clientRef.ask(HExists(key, field)).mapTo[HExists#Ret]

  def hdel(key: String, field: String, fields: String*)(implicit timeout: Timeout) =
    clientRef.ask(HDel(key, field, fields:_*)).mapTo[HDel#Ret]

  def hlen(key: String)(implicit timeout: Timeout) =
    clientRef.ask(HLen(key)).mapTo[HLen#Ret]

  def hkeys[A](key: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(HKeys[A](key)).mapTo[HKeys[A]#Ret]

  def hvals[A](key: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(HVals(key)).mapTo[HVals[A]#Ret]

  def hgetall[A](key: String)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(HGetall(key)).mapTo[HGetall[A]#Ret]
}
