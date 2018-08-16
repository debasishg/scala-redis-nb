package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.HashCommands

trait HashOperations { this: RedisOps =>
  import HashCommands._

  def hset(key: String, field: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(HSet(key, field, value)).mapTo[HSet#Ret]

  def hsetnx(key: String, field: String, value: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(HSetNx(key, field, value)).mapTo[HSetNx#Ret]

  def hget[A](key: String, field: String)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(HGet[A](key, field)).mapTo[HGet[A]#Ret]

  def hmset(key: String, mapLike: Iterable[KeyValuePair])(implicit timeout: Timeout) =
    clientRef.ask(HMSet(key, mapLike)).mapTo[HMSet#Ret]


  def hmget[A](key: String, fields: Seq[String])(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(HMGet[A](key, fields)).mapTo[HMGet[A]#Ret]

  def hmget[A](key: String, field: String, fields: String*)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(HMGet[A](key, field, fields:_*)).mapTo[HMGet[A]#Ret]


  def hincrby(key: String, field: String, value: Int)(implicit timeout: Timeout) =
    clientRef.ask(HIncrby(key, field, value)).mapTo[HIncrby#Ret]

  // HINCRBYFLOAT (key, field, increment)
  // Increment the specified field of an hash stored at key, and representing a floating point number, by the specified increment
  def hincrbyfloat(key: String, field: String, value: Double)(implicit timeout: Timeout) =
    clientRef.ask(HIncrByFloat(key, field, value)).mapTo[HIncrByFloat#Ret]

  def hexists(key: String, field: String)(implicit timeout: Timeout) =
    clientRef.ask(HExists(key, field)).mapTo[HExists#Ret]


  def hdel(key: String, fields: Seq[String])(implicit timeout: Timeout) =
    clientRef.ask(HDel(key, fields)).mapTo[HDel#Ret]

  def hdel(key: String, field: String, fields: String*)(implicit timeout: Timeout) =
    clientRef.ask(HDel(key, field, fields:_*)).mapTo[HDel#Ret]


  def hlen(key: String)(implicit timeout: Timeout) =
    clientRef.ask(HLen(key)).mapTo[HLen#Ret]

  def hkeys(key: String)(implicit timeout: Timeout) =
    clientRef.ask(HKeys(key)).mapTo[HKeys#Ret]

  def hvals[A](key: String)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(HVals(key)).mapTo[HVals[A]#Ret]

  def hgetall[A](key: String)(implicit timeout: Timeout, reader: Reader[A]) =
    clientRef.ask(HGetall(key)).mapTo[HGetall[A]#Ret]
}
