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
  def zadd[A](key: String, score: Double, member: A, scoreVals: (Double, A)*)
          (implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(ZAdd[A](key, score, member, scoreVals:_*)).mapTo[ZAdd[A]#Ret]

  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  def zrem[A](key: String, member: A, members: A*)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(ZRem[A](key, member, members:_*)).mapTo[ZRem[A]#Ret]

  // ZINCRBY
  //
  def zincrby[A](key: String, incr: Double, member: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(ZIncrby[A](key, incr, member)).mapTo[ZIncrby[A]#Ret]

  // ZCARD
  //
  def zcard(key: String)(implicit timeout: Timeout) =
    clientRef.ask(ZCard(key)).mapTo[ZCard#Ret]

  // ZSCORE
  //
  def zscore[A](key: String, element: A)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(ZScore[A](key, element)).mapTo[ZScore[A]#Ret]

  // ZRANGE
  //
  def zrange[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(ZRange[A](key, start, end, sortAs)).mapTo[ZRange[A]#Ret]

  def zrangeWithScore[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(ZRangeWithScore[A](key, start, end, sortAs)).mapTo[ZRangeWithScore[A]#Ret]

  // ZRANGEBYSCORE
  //
  def zrangeByScore[A](key: String,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(ZRangeByScore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[ZRangeByScore[A]#Ret]

  def zrangeByScoreWithScore[A](key: String,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit timeout: Timeout, read: Read[A]) =
    clientRef.ask(ZRangeByScoreWithScore[A](key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[ZRangeByScoreWithScore[A]#Ret]

  // ZRANK
  // ZREVRANK
  //
  def zrank[A](key: String, member: A, reverse: Boolean = false)(implicit timeout: Timeout, write: Write[A]) =
    clientRef.ask(ZRank[A](key, member, reverse)).mapTo[ZRank[A]#Ret]

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
