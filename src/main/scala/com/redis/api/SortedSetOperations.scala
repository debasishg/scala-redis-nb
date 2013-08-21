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
  def zadd(key: String, scoreMembers: Seq[ScoredValue])
          (implicit timeout: Timeout) =
    clientRef.ask(ZAdd(key, scoreMembers)).mapTo[ZAdd#Ret]

  def zadd(key: String, score: Double, member: Stringified)
          (implicit timeout: Timeout) =
    clientRef.ask(ZAdd(key, score, member)).mapTo[ZAdd#Ret]

  def zadd(key: String, scoreMember: ScoredValue, scoreMembers: ScoredValue*)
          (implicit timeout: Timeout) =
    clientRef.ask(ZAdd(key, scoreMember +: scoreMembers)).mapTo[ZAdd#Ret]


  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  def zrem(key: String, members: Seq[Stringified])(implicit timeout: Timeout) =
    clientRef.ask(ZRem(key, members)).mapTo[ZRem#Ret]

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
    clientRef.ask(ZRange[A](key, start, end)).mapTo[ZRange[A]#Ret]

  def zrevrange[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
               (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ZRevRange[A](key, start, end)).mapTo[ZRevRange[A]#Ret]

  def zrangeWithScores[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                        (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ZRangeWithScores[A](key, start, end)).mapTo[ZRangeWithScores[A]#Ret]

  def zrevrangeWithScores[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                          (implicit timeout: Timeout, reader: Read[A]) =
    clientRef.ask(ZRevRangeWithScores[A](key, start, end)).mapTo[ZRevRangeWithScores[A]#Ret]

  // ZRANGEBYSCORE
  //
  def zrangeByScore[A](key: String,
                       min: Double = Double.NegativeInfinity, minInclusive: Boolean = true,
                       max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true,
                       limit: Option[(Int, Int)])
                      (implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRangeByScore[A](key, min, minInclusive, max, maxInclusive, limit))
      .mapTo[ZRangeByScore[A]#Ret]

  def zrevrangeByScore[A](key: String,
                          max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true,
                          min: Double = Double.NegativeInfinity, minInclusive: Boolean = true,
                          limit: Option[(Int, Int)])
                         (implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRevRangeByScore[A](key, min, minInclusive, max, maxInclusive, limit))
      .mapTo[ZRevRangeByScore[A]#Ret]


  def zrangeByScoreWithScores[A](key: String,
                                 min: Double = Double.NegativeInfinity, minInclusive: Boolean = true,
                                 max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true,
                                 limit: Option[(Int, Int)])
                                (implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRangeByScoreWithScores[A](key, min, minInclusive, max, maxInclusive, limit))
      .mapTo[ZRangeByScoreWithScores[A]#Ret]

  def zrevrangeByScoreWithScores[A](key: String,
                                    max: Double = Double.PositiveInfinity, maxInclusive: Boolean = true,
                                    min: Double = Double.NegativeInfinity, minInclusive: Boolean = true,
                                    limit: Option[(Int, Int)])
                                   (implicit timeout: Timeout, reader: Read[A]) =
    clientRef
      .ask(ZRevRangeByScoreWithScores[A](key, min, minInclusive, max, maxInclusive, limit))
      .mapTo[ZRevRangeByScoreWithScores[A]#Ret]

  // ZRANK
  // ZREVRANK
  //
  def zrank(key: String, member: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(ZRank(key, member)).mapTo[ZRank#Ret]

  def zrevrank(key: String, member: Stringified)(implicit timeout: Timeout) =
    clientRef.ask(ZRevRank(key, member)).mapTo[ZRank#Ret]


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
    clientRef.ask(ZUnionStore(dstKey, keys, aggregate)).mapTo[ZInterStore#Ret]

  // ZINTERSTORE
  //
  def zinterstore(dstKey: String, keys: Iterable[String], aggregate: Aggregate = SUM)(implicit timeout: Timeout) =
    clientRef.ask(ZInterStore(dstKey, keys, aggregate)).mapTo[ZInterStore#Ret]

  def zunionstoreweighted(dstKey: String, kws: Iterable[Product2[String, Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout) =
    clientRef.ask(ZUnionStoreWeighted(dstKey, kws, aggregate)).mapTo[ZUnionStoreWeighted#Ret]

  def zinterstoreweighted(dstKey: String, kws: Iterable[Product2[String, Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout) =
    clientRef.ask(ZInterStoreWeighted(dstKey, kws, aggregate)).mapTo[ZInterStoreWeighted#Ret]

  // ZCOUNT
  //
  def zcount(key: String, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
             minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit timeout: Timeout) =
    clientRef.ask(ZCount(key, min, max, minInclusive, maxInclusive)).mapTo[ZCount#Ret]
}
