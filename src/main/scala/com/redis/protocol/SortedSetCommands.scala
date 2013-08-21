package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object SortedSetCommands {

  val `+Inf` = Double.PositiveInfinity
  val `-Inf` = Double.NegativeInfinity


  case class ZAdd(key: String, scoreMembers: Seq[ScoredValue]) extends RedisCommand[Long] {
    require(scoreMembers.nonEmpty)
    def line = multiBulk(
      "ZADD" +: key +: scoreMembers.foldRight(List[String]())((x, acc) => x.score.toString +: x.value.toString +: acc)
    )
  }

  object ZAdd {
    def apply(key: String, score: Double, member: Stringified): ZAdd =
      ZAdd(key, Seq(ScoredValue(score, member)))

    def apply(key: String, scoreMember: ScoredValue, scoreMembers: ScoredValue*): ZAdd =
      ZAdd(key, scoreMember +: scoreMembers)
  }


  case class ZRem(key: String, members: Seq[Stringified]) extends RedisCommand[Long] {
    require(members.nonEmpty)
    def line = multiBulk("ZREM" +: key +: members.map(_.toString))
  }

  object ZRem {
    def apply(key: String, member: Stringified, members: Stringified*): ZRem = ZRem(key, member +: members)
  }

  
  case class ZIncrby(key: String, incr: Double, member: Stringified) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZINCRBY" +: Seq(key, incr.toString, member.toString))
  }
  
  case class ZCard(key: String) extends RedisCommand[Long] {
    def line = multiBulk("ZCARD" +: Seq(key))
  }
  
  case class ZScore(key: String, element: Stringified) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZSCORE" +: Seq(key, element.toString))
  }


  case class ZRange[A](key: String, start: Int = 0, end: Int = -1)
                      (implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("ZRANGE" +: key +: start.toString +: end.toString +: Nil)

    def reverse = ZRevRange(key, start, end)

    def withScores = ZRangeWithScores[A](key, start, end)
  }

  case class ZRangeWithScores[A](key: String, start: Int = 0, end: Int = -1)
                                (implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {
    def line = multiBulk("ZRANGE" +: key +: start.toString +: end.toString +: "WITHSCORES" +: Nil)

    def reverse = ZRangeWithScores(key, start, end)
  }


  case class ZRevRange[A](key: String, start: Int = 0, end: Int = -1)
                      (implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("ZREVRANGE" +: key +: start.toString +: end.toString +: Nil)

    def withScores = ZRevRangeWithScores[A](key, start, end)
  }

  case class ZRevRangeWithScores[A](key: String, start: Int = 0, end: Int = -1)
                         (implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {
    def line = multiBulk("ZREVRANGE" +: key +: start.toString +: end.toString +: "WITHSCORES" +: Nil)
  }


  case class ZRangeByScore[A](key: String,
                              min: Double = `-Inf`, minInclusive: Boolean = true,
                              max: Double = `+Inf`, maxInclusive: Boolean = true,
                              limit: Option[(Int, Int)] = None)
                             (implicit reader: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk("ZRANGEBYSCORE" +: key +: scoreParams(min, minInclusive, max, maxInclusive, limit, false))

    def reverse = ZRevRangeByScore(key, min, minInclusive, max, maxInclusive, limit)

    def withScores = ZRangeByScoreWithScores[A](key, min, minInclusive, max, maxInclusive, limit)
  }

  case class ZRangeByScoreWithScores[A](key: String,
                                        min: Double = `-Inf`, minInclusive: Boolean = true,
                                        max: Double = `+Inf`, maxInclusive: Boolean = true,
                                        limit: Option[(Int, Int)] = None)
                                       (implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {

    def line = multiBulk("ZRANGEBYSCORE" +: key +: scoreParams(min, minInclusive, max, maxInclusive, limit, true))

    def reverse = ZRevRangeByScoreWithScores(key, min, minInclusive, max, maxInclusive, limit)
  }

  case class ZRevRangeByScore[A](key: String,
                                 max: Double = `+Inf`, maxInclusive: Boolean = true,
                                 min: Double = `-Inf`, minInclusive: Boolean = true,
                                 limit: Option[(Int, Int)] = None)
                                (implicit reader: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk("ZREVRANGEBYSCORE" +: key +: scoreParams(max, maxInclusive, min, minInclusive, limit, false))

    def withScores = ZRevRangeByScoreWithScores(key, min, minInclusive, max, maxInclusive, limit)
  }

  case class ZRevRangeByScoreWithScores[A](key: String,
                                           max: Double = `+Inf`, maxInclusive: Boolean = true,
                                           min: Double = `-Inf`, minInclusive: Boolean = true,
                                           limit: Option[(Int, Int)] = None)
                                          (implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {

    def line = multiBulk("ZREVRANGEBYSCORE" +: key +: scoreParams(max, maxInclusive, min, minInclusive, limit, true))
  }

  private def scoreParams(from: Double, fromInclusive: Boolean, to: Double, toInclusive: Boolean,
                          limit: Option[(Int, Int)], withScores: Boolean): Seq[String] = {

    import Write.Internal.formatDouble

    formatDouble(from, fromInclusive) +: formatDouble(to, toInclusive) +: (
      (if (withScores) Seq("WITHSCORES") else Seq.empty[String]) ++
      (limit match {
        case Some((from, to)) => "LIMIT" +: from.toString +: to.toString +: Nil
        case _ => Seq.empty[String]
      })
    )
  }


  case class ZRank(key: String, member: Stringified)
                   extends RedisCommand[Long] {
    def line = multiBulk("ZRANK" +: key +: member.toString +: Nil)

    def reverse = ZRevRank(key, member)
  }

  case class ZRevRank(key: String, member: Stringified)
    extends RedisCommand[Long] {
    def line = multiBulk("ZREVRANK" +: key +: member.toString +: Nil)
  }


  case class ZRemRangeByRank(key: String, start: Int = 0, end: Int = -1) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYRANK" +: key +: start.toString +: end.toString +: Nil)
  }

  case class ZRemRangeByScore(key: String, start: Double = `-Inf`, end: Double = `+Inf`) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYSCORE" +: key +: start.toString +: end.toString +: Nil)
  }


  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  case class ZInterStore(dstKey: String, keys: Iterable[String],
                         aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk("ZINTERSTORE" +:
      (Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toSeq)
  }

  case class ZUnionStore(dstKey: String, keys: Iterable[String],
                         aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk("ZUNIONSTORE" +:
      (Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toSeq)
  }

  case class ZInterStoreWeighted(dstKey: String, kws: Iterable[Product2[String, Double]],
                                 aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk("ZINTERSTORE" +:
      (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
        kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toSeq
    )
  }

  case class ZUnionStoreWeighted(dstKey: String, kws: Iterable[Product2[String, Double]],
                                 aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk("ZUNIONSTORE" +:
      (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
        kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toSeq
    )
  }

  case class ZCount(key: String,
                    min: Double = `-Inf`, minInclusive: Boolean = true,
                    max: Double = `+Inf`, maxInclusive: Boolean = true) extends RedisCommand[Long] {
    import Write.Internal.formatDouble

    def line =
      multiBulk("ZCOUNT" +: key +: formatDouble(min, minInclusive) +: formatDouble(max, maxInclusive) +: Nil)
  }
}
