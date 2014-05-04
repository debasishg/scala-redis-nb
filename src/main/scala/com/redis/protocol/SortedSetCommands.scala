package com.redis.protocol

import com.redis.serialization._


object SortedSetCommands {
  import DefaultWriters._


  final val `+Inf` = Double.PositiveInfinity
  final val `-Inf` = Double.NegativeInfinity
  final val `+LexInf`   = "+"
  final val `-LexInf`   = "-"

  case class ZAdd(key: String, scoreMembers: Seq[ScoredValue]) extends RedisCommand[Long]("ZADD") {
    require(scoreMembers.nonEmpty)
    def params = key +: scoreMembers.foldRight (ANil) { (x, acc) => x.score +: x.value +: acc }
  }

  object ZAdd {
    def apply(key: String, score: Double, member: Stringified): ZAdd =
      ZAdd(key, Seq(ScoredValue(score, member)))

    def apply(key: String, scoreMember: ScoredValue, scoreMembers: ScoredValue*): ZAdd =
      ZAdd(key, scoreMember +: scoreMembers)
  }


  case class ZRem(key: String, members: Seq[Stringified]) extends RedisCommand[Long]("ZREM") {
    require(members.nonEmpty)
    def params = key +: members.toArgs
  }

  object ZRem {
    def apply(key: String, member: Stringified, members: Stringified*): ZRem = ZRem(key, member +: members)
  }

  
  case class ZIncrby(key: String, incr: Double, member: Stringified) extends RedisCommand[Option[Double]]("ZINCRBY") {
    def params = key +: incr +: member +: ANil
  }
  
  case class ZCard(key: String) extends RedisCommand[Long]("ZCARD") {
    def params = key +: ANil
  }
  
  case class ZScore(key: String, element: Stringified) extends RedisCommand[Option[Double]]("ZSCORE") {
    def params = key +: element +: ANil
  }


  case class ZRange[A](key: String, start: Int = 0, end: Int = -1)(implicit reader: Reader[A])
                      extends RedisCommand[List[A]]("ZRANGE") {
    def params = key +: start +: end +: ANil

    def reverse = ZRevRange(key, start, end)(reader)

    def withScores = ZRangeWithScores[A](key, start, end)(reader)
  }

  case class ZRangeWithScores[A](key: String, start: Int = 0, end: Int = -1)(implicit reader: Reader[A])
                                extends RedisCommand[List[(A, Double)]]("ZRANGE") {
    def params = key +: start +: end +: "WITHSCORES" +: ANil

    def reverse = ZRangeWithScores(key, start, end)(reader)
  }


  case class ZRevRange[A](key: String, start: Int = 0, end: Int = -1)(implicit reader: Reader[A])
                      extends RedisCommand[List[A]]("ZREVRANGE") {
    def params = key +: start +: end +: ANil

    def withScores = ZRevRangeWithScores[A](key, start, end)(reader)
  }

  case class ZRevRangeWithScores[A: Reader](key: String, start: Int = 0, end: Int = -1)
                                   extends RedisCommand[List[(A, Double)]]("ZREVRANGE") {
    def params = key +: start +: end +: "WITHSCORES" +: ANil
  }


  case class ZRangeByScore[A](key: String,
                              min: Double = `-Inf`, minInclusive: Boolean = true,
                              max: Double = `+Inf`, maxInclusive: Boolean = true,
                              limit: Option[(Int, Int)] = None)(implicit reader: Reader[A])
                             extends RedisCommand[List[A]]("ZRANGEBYSCORE") {

    def params = key +: scoreParams(min, minInclusive, max, maxInclusive, limit, false)

    def reverse = ZRevRangeByScore(key, min, minInclusive, max, maxInclusive, limit)(reader)

    def withScores = ZRangeByScoreWithScores[A](key, min, minInclusive, max, maxInclusive, limit)(reader)
  }

  case class ZRangeByScoreWithScores[A](key: String,
                                        min: Double = `-Inf`, minInclusive: Boolean = true,
                                        max: Double = `+Inf`, maxInclusive: Boolean = true,
                                        limit: Option[(Int, Int)] = None)(implicit reader: Reader[A])
                                        extends RedisCommand[List[(A, Double)]]("ZRANGEBYSCORE") {

    def params = key +: scoreParams(min, minInclusive, max, maxInclusive, limit, true)

    def reverse = ZRevRangeByScoreWithScores(key, min, minInclusive, max, maxInclusive, limit)(reader)
  }

  case class ZRevRangeByScore[A](key: String,
                                 max: Double = `+Inf`, maxInclusive: Boolean = true,
                                 min: Double = `-Inf`, minInclusive: Boolean = true,
                                 limit: Option[(Int, Int)] = None)(implicit reader: Reader[A])
                                extends RedisCommand[List[A]]("ZREVRANGEBYSCORE") {

    def params = key +: scoreParams(max, maxInclusive, min, minInclusive, limit, false)

    def withScores = ZRevRangeByScoreWithScores(key, min, minInclusive, max, maxInclusive, limit)(reader)
  }

  case class ZRevRangeByScoreWithScores[A: Reader](key: String,
                                           max: Double = `+Inf`, maxInclusive: Boolean = true,
                                           min: Double = `-Inf`, minInclusive: Boolean = true,
                                           limit: Option[(Int, Int)] = None)
                                          extends RedisCommand[List[(A, Double)]]("ZREVRANGEBYSCORE") {

    def params = key +: scoreParams(max, maxInclusive, min, minInclusive, limit, true)
  }

  private def scoreParams(from: Double, fromInclusive: Boolean, to: Double, toInclusive: Boolean,
                          limit: Option[(Int, Int)], withScores: Boolean): Args = {

    formatDouble(from, fromInclusive) +: formatDouble(to, toInclusive) +: (
      (if (withScores) Seq("WITHSCORES") else Nil) ++:
      (limit match {
        case Some((from, to)) => "LIMIT" +: from +: to +: ANil
        case _ => ANil
      })
    )
  }


  case class ZRank(key: String, member: Stringified)
      extends RedisCommand[Option[Long]]("ZRANK")(PartialDeserializer.liftOptionPD[Long]) {
    def params = key +: member +: ANil

    def reverse = ZRevRank(key, member)
  }

  case class ZRevRank(key: String, member: Stringified)
      extends RedisCommand[Option[Long]]("ZREVRANK")(PartialDeserializer.liftOptionPD[Long]) {
    def params = key +: member +: ANil
  }


  case class ZRemRangeByRank(key: String, start: Int = 0, end: Int = -1) extends RedisCommand[Long]("ZREMRANGEBYRANK") {
    def params = key +: start +: end +: ANil
  }

  case class ZRemRangeByScore(key: String, start: Double = `-Inf`, end: Double = `+Inf`) extends RedisCommand[Long]("ZREMRANGEBYSCORE") {
    def params = key +: start +: end +: ANil
  }


  sealed trait Aggregate
  case object SUM extends Aggregate
  case object MIN extends Aggregate
  case object MAX extends Aggregate

  case class ZInterStore(dstKey: String, keys: Iterable[String],
                         aggregate: Aggregate = SUM) extends RedisCommand[Long]("ZINTERSTORE") {

    def params =
      (Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toSeq.toArgs
  }

  case class ZUnionStore(dstKey: String, keys: Iterable[String],
                         aggregate: Aggregate = SUM) extends RedisCommand[Long]("ZUNIONSTORE") {

    def params =
      (Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toSeq.toArgs
  }

  case class ZInterStoreWeighted(dstKey: String, kws: Iterable[Product2[String, Double]],
                                 aggregate: Aggregate = SUM) extends RedisCommand[Long]("ZINTERSTORE") {

    def params =
      (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
        kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toSeq.toArgs
  }

  case class ZUnionStoreWeighted(dstKey: String, kws: Iterable[Product2[String, Double]],
                                 aggregate: Aggregate = SUM) extends RedisCommand[Long]("ZUNIONSTORE") {

    def params =
      (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
        kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toSeq.toArgs
  }

  case class ZCount(key: String,
                    min: Double = `-Inf`, minInclusive: Boolean = true,
                    max: Double = `+Inf`, maxInclusive: Boolean = true) extends RedisCommand[Long]("ZCOUNT") {

    def params = key +: formatDouble(min, minInclusive) +: formatDouble(max, maxInclusive) +: ANil
  }

  case class ZLexCount(key: String, 
                       minKey: String = `-LexInf`, minInclusive: Boolean = true,
                       maxKey: String = `+LexInf`, maxInclusive: Boolean = true) extends RedisCommand[Long]("ZLEXCOUNT") {
    def params = key +: formatLex(minKey, minInclusive) +: formatLex(maxKey, maxInclusive) +: ANil 
  }

  case class ZRangeByLex[A](key: String,
                            min: String, minInclusive: Boolean = true,
                            max: String, maxInclusive: Boolean = true,
                            limit: Option[(Int, Int)] = None)(implicit reader: Reader[A])
                            extends RedisCommand[List[A]]("ZRANGEBYLEX") {

    def params = key +: lexParams(min, minInclusive, max, maxInclusive, limit)
  }

  case class ZRemRangeByLex[A](key: String,
                               min: String, minInclusive: Boolean = true,
                               max: String, maxInclusive: Boolean = true)
                               extends RedisCommand[Long]("ZREMRANGEBYLEX") {

    def params = key +: lexParams(min, minInclusive, max, maxInclusive)
  }

  private def lexParams(min: String, minInclusive: Boolean, max: String, maxInclusive: Boolean,
                        limit: Option[(Int, Int)] = None): Args = {

    formatLex(min, minInclusive) +: formatLex(max, maxInclusive) +: 
    (
      limit match {
        case Some((from, to)) => "LIMIT" +: from +: to +: ANil
        case _ => ANil
      }
    )
  }

  private def formatLex(key: String, inclusive: Boolean) = Stringified(
    if (key == `+LexInf` || key == `-LexInf`) key
    else {
      if (inclusive) s"[$key" else s"($key"
    }
  )

  private def formatDouble(d: Double, inclusive: Boolean = true) = Stringified(
    (if (inclusive) ("") else ("(")) + {
      if (d.isInfinity) {
        if (d > 0.0) "+inf" else "-inf"
      } else {
        d.toString
      }
    }
  )
}
