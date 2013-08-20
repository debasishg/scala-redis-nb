package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.{RedisCommand, SortedSetCommands}
import RedisCommand._


trait SortedSetOperations { this: RedisOps =>
  import SortedSetCommands._

  // ZADD (Variadic: >= 2.4)
  // Add the specified members having the specified score to the sorted set stored at key.
  def zadd(key: String, score: Double, member: Stringified, scoreVals: ScoredValue*)
          (implicit timeout: Timeout) =
    clientRef.ask(ZAdd(key, score, member, scoreVals:_*)).mapTo[ZAdd#Ret]

  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  def zrem(key: String, member: Stringified, members: Stringified*)(implicit timeout: Timeout) =
    clientRef.ask(ZRem(key, member, members:_*)).mapTo[ZRem#Ret]

  // ZINCRBY
  //
  def zincrby(key: String, incr: Double, member: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(ZIncrby(key, incr, member)).mapTo[ZIncrby#Ret]

  // ZCARD
  //
  def zcard(key: String)(implicit timeout: Timeout) =
    clientRef.ask(ZCard(key)).mapTo[ZCard#Ret]

  // ZSCORE
  //
  def zscore(key: String, element: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(ZScore(key, element)).mapTo[ZScore#Ret]

  // ZRANGE
  //
  def zrange[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
               (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ZRange[A](key, start, end, sortAs)).mapTo[ZRange[A]#Ret]

  def zrangeWithScore[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                        (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ZRangeWithScore[A](key, start, end, sortAs)).mapTo[ZRangeWithScore[A]#Ret]

  // ZRANGEBYSCORE
  //
  def zrangeByScore[A](key: String,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRangeByScore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))
      .mapTo[ZRangeByScore[A]#Ret]

  def zrangeByScoreWithScore[A](key: String,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRangeByScoreWithScore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs))
      .mapTo[ZRangeByScoreWithScore[A]#Ret]

  // ZRANK
  // ZREVRANK
  //
  def zrank(key: String, member: Stringified, reverse: Boolean = false)(implicit timeout: Timeout) =
    clientRef.ask(ZRank(key, member, reverse)).mapTo[ZRank#Ret]

  // ZREMRANGEBYRANK
  //
  def zremrangebyrank(key: String, start: Int = 0, end: Int = -1)(implicit timeout: Timeout) =
    clientRef.ask(ZRemRangeByRank(key, start, end)).mapTo[ZRemRangeByRank#Ret]

  // ZREMRANGEBYSCORE
  //
  def zremrangebyscore(key: String, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)
                      (implicit timeout: Timeout) =
    clientRef.ask(ZRemRangeByScore(key, start, end)).mapTo[ZRemRangeByScore#Ret]

  // ZUNION
  //
  def zunionstore(dstKey: String, keys: Iterable[String], aggregate: Aggregate = SUM)(implicit timeout: Timeout) =
    clientRef.ask(ZUnionInterStore(union, dstKey, keys, aggregate)).mapTo[ZUnionInterStore#Ret]

  // ZINTERSTORE
  //
  def zinterstore(dstKey: String, keys: Iterable[String], aggregate: Aggregate = SUM)(implicit timeout: Timeout) =
    clientRef.ask(ZUnionInterStore(inter, dstKey, keys, aggregate)).mapTo[ZUnionInterStore#Ret]

  def zunionstoreweighted(dstKey: String, kws: Iterable[Product2[String, Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout) =
    clientRef.ask(ZUnionInterStoreWeighted(union, dstKey, kws, aggregate)).mapTo[ZUnionInterStoreWeighted#Ret]

  def zinterstoreweighted(dstKey: String, kws: Iterable[Product2[String, Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout) =
    clientRef.ask(ZUnionInterStoreWeighted(inter, dstKey, kws, aggregate)).mapTo[ZUnionInterStoreWeighted#Ret]

  // ZCOUNT
  //
  def zcount(key: String, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
             minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit timeout: Timeout) =
    clientRef.ask(ZCount(key, min, max, minInclusive, maxInclusive)).mapTo[ZCount#Ret]
}
