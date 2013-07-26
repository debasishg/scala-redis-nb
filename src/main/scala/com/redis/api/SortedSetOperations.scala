package com.redis
package api

import serialization._
import akka.pattern.ask
import akka.util.Timeout
import com.redis.protocol.RedisCommand
import RedisCommand._
import com.redis.protocol.SortedSetCommands

trait SortedSetOperations { this: RedisOps =>
  import SortedSetCommands._

  // ZADD (Variadic: >= 2.4)
  // Add the specified members having the specified score to the sorted set stored at key.
  def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)
          (implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZAdd(key, score, member, scoreVals:_*)).mapTo[Long]

  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  def zrem(key: Any, member: Any, members: Any*)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZRem(key, member, members:_*)).mapTo[Long]

  // ZINCRBY
  //
  def zincrby(key: Any, incr: Double, member: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZIncrby(key, incr, member)).mapTo[Option[Double]]

  // ZCARD
  //
  def zcard(key: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZCard(key)).mapTo[Long]

  // ZSCORE
  //
  def zscore(key: Any, element: Any)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZScore(key, element)).mapTo[Option[Double]]

  // ZRANGE
  //
  def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(ZRange(key, start, end, sortAs)).mapTo[List[A]]

  def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(ZRangeWithScore(key, start, end, sortAs)).mapTo[List[(A, Double)]]

  // ZRANGEBYSCORE
  //
  def zrangeByScore[A](key: Any,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(ZRangeByScore(key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[List[A]]

  def zrangeByScoreWithScore[A](key: Any,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit timeout: Timeout, format: Format, parse: Parse[A]) =
    clientRef.ask(ZRangeByScoreWithScore(key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[List[(A, Double)]]

  // ZRANK
  // ZREVRANK
  //
  def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZRank(key, member, reverse)).mapTo[Long]

  // ZREMRANGEBYRANK
  //
  def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZRemRangeByRank(key, start, end)).mapTo[Long]

  // ZREMRANGEBYSCORE
  //
  def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)
                      (implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZRemRangeByScore(key, start, end)).mapTo[Long]

  // ZUNION
  //
  def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZUnionInterStore(union, dstKey, keys, aggregate)).mapTo[Long]

  // ZINTERSTORE
  //
  def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZUnionInterStore(inter, dstKey, keys, aggregate)).mapTo[Long]

  def zunionstoreweighted(dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZUnionInterStoreWeighted(union, dstKey, kws, aggregate)).mapTo[Long]

  def zinterstoreweighted(dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
                         (implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZUnionInterStoreWeighted(inter, dstKey, kws, aggregate)).mapTo[Long]

  // ZCOUNT
  //
  def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
             minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit timeout: Timeout, format: Format) =
    clientRef.ask(ZCount(key, min, max, minInclusive, maxInclusive)).mapTo[Long]
}
