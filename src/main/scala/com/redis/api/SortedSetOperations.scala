package com.redis
package api

import scala.concurrent.Future
import serialization._
import akka.pattern.ask
import akka.actor._
import akka.util.Timeout
import RedisCommand._

trait SortedSetOperations {
  import SortedSetCommands._

  implicit val timeout: Timeout

  // ZADD (Variadic: >= 2.4)
  // Add the specified members having the specified score to the sorted set stored at key.
  def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>

    client.ask(ZAdd(key, score, member, scoreVals:_*)).mapTo[Option[Long]] 
  }
  
  // ZREM (Variadic: >= 2.4)
  // Remove the specified members from the sorted set value stored at key.
  def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZRem(key, member, members:_*)).mapTo[Option[Long]] 
  }
  
  // ZINCRBY
  // 
  def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): ActorRef => Future[Option[Double]] = {client: ActorRef =>
    client.ask(ZIncrby(key, incr, member)).mapTo[Option[Double]] 
  }
  
  // ZCARD
  // 
  def zcard(key: Any)(implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZCard(key)).mapTo[Option[Long]] 
  }
  
  // ZSCORE
  // 
  def zscore(key: Any, element: Any)(implicit format: Format): ActorRef => Future[Option[Double]] = {client: ActorRef =>
    client.ask(zscore(key, element)).mapTo[Option[Double]] 
  }

  // ZRANGE
  // 
  def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) 
   : ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>
    client.ask(ZRange(key, start, end, sortAs)).mapTo[Option[List[Option[A]]]] 
  }

  def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A])
   : ActorRef => Future[Option[List[Option[(A, Double)]]]] = {client: ActorRef =>
    client.ask(ZRangeWithScore(key, start, end, sortAs)).mapTo[Option[List[Option[(A, Double)]]]] 
  }

  // ZRANGEBYSCORE
  // 
  def zrangeByScore[A](key: Any,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]): ActorRef => Future[Option[List[Option[A]]]] = {client: ActorRef =>

    client.ask(ZRangeByScore(key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[Option[List[Option[A]]]] 
  }

  def zrangeByScoreWithScore[A](key: Any,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A])
    : ActorRef => Future[Option[List[Option[(A, Double)]]]] = {client: ActorRef =>

    client.ask(ZRangeByScoreWithScore(key, min, minInclusive, max, maxInclusive, limit, sortAs)).mapTo[Option[List[Option[(A, Double)]]]] 
  }

  // ZRANK
  // ZREVRANK
  //
  def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format)
    : ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZRank(key, member, reverse)).mapTo[Option[Long]] 
  }

  // ZREMRANGEBYRANK
  //
  def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format)
    : ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZRemRangeByRank(key, start, end)).mapTo[Option[Long]] 
  }

  // ZREMRANGEBYSCORE
  //
  def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZRemRangeByScore(key, start, end)).mapTo[Option[Long]] 
  }

  // ZUNION
  //
  def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format) 
   : ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZUnionInterStore(union, dstKey, keys, aggregate)).mapTo[Option[Long]] 
  }

  // ZINTERSTORE
  //
  def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format) 
   : ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZUnionInterStore(inter, dstKey, keys, aggregate)).mapTo[Option[Long]] 
  }

  def zunionstoreweighted(dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZUnionInterStoreWeighted(union, dstKey, kws, aggregate)).mapTo[Option[Long]] 
  }

  def zinterstoreweighted(dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
    (implicit format: Format): ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZUnionInterStoreWeighted(inter, dstKey, kws, aggregate)).mapTo[Option[Long]] 
  }

  // ZCOUNT
  //
  def zcount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, 
    minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format)
    : ActorRef => Future[Option[Long]] = {client: ActorRef =>
    client.ask(ZCount(key, min, max, minInclusive, maxInclusive)).mapTo[Option[Long]] 
  }
}
