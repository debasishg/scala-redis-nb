package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object SortedSetCommands {
  case class ZAdd(key: String, score: Double, member: Stringified, scoreVals: ScoredValue*)
      extends RedisCommand[Long] {

    def line = multiBulk(
      "ZADD" +: key +: score.toString +: member.toString
        +: scoreVals.foldRight(List[String]())((x, acc) => x.score.toString +: x.value.toString +: acc)
    )
  }
  
  case class ZRem(key: String, member: Stringified, members: Stringified*) extends RedisCommand[Long] {
    def line = multiBulk("ZREM" +: key +: (member +: members).map(_.toString))
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

  case class ZRange[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                      (implicit reader: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +: key +: Seq(start, end).map(_.toString))
  }

  case class ZRangeWithScore[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                               (implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {
    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +:
        Seq(key, start.toString, end.toString, "WITHSCORES")
    )
  }

  case class ZRangeByScore[A](key: String,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit reader: Read[A]) extends RedisCommand[List[A]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" +: key +: minParam +: maxParam +: limitEntries
      else "ZREVRANGEBYSCORE" +: key +: maxParam +: minParam +: limitEntries
    )
  }

  case class ZRangeByScoreWithScore[A](key: String,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit reader: Read[A]) extends RedisCommand[List[(A, Double)]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" +: key +: minParam +: maxParam +: "WITHSCORES" +: limitEntries
      else "ZREVRANGEBYSCORE" +: key +: maxParam +: minParam +: "WITHSCORES" +: limitEntries
    )
  }

  private def zrangebyScoreWithScoreInternal(
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)]): (Seq[String], String, String) = {

    val limitEntries = limit.fold(Seq.empty[String]) { case (from, to) => "LIMIT" +: Seq(from, to).map(_.toString) }

    val minParam = Write.Internal.formatDouble(min, minInclusive)
    val maxParam = Write.Internal.formatDouble(max, maxInclusive)
    (limitEntries, minParam, maxParam)
  }

  case class ZRank(key: String, member: Stringified, reverse: Boolean = false)
                   extends RedisCommand[Long] {
    def line = multiBulk((if (reverse) "ZREVRANK" else "ZRANK") +: Seq(key, member.toString))
  }

  case class ZRemRangeByRank(key: String, start: Int = 0, end: Int = -1) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYRANK" +: key +: Seq(start, end).map(_.toString))
  }

  case class ZRemRangeByScore(key: String,
                              start: Double = Double.NegativeInfinity,
                              end: Double = Double.PositiveInfinity) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYSCORE" +: key +: Seq(start, end).map(_.toString))
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp

  case class ZUnionInterStore(ux: setOp, dstKey: String, keys: Iterable[String],
                              aggregate: Aggregate = SUM) extends RedisCommand[Long] {
    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
      ((Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toSeq)
    )
  }

  case class ZUnionInterStoreWeighted(ux: setOp, dstKey: String, kws: Iterable[Product2[String, Double]],
                                      aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
        (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
          kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toSeq
    )
  }

  case class ZCount(key: String, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
                    minInclusive: Boolean = true, maxInclusive: Boolean = true) extends RedisCommand[Long] {
    def line =
      multiBulk("ZCOUNT" +: key +:
        Write.Internal.formatDouble(min, minInclusive) +: Write.Internal.formatDouble(max, maxInclusive) +: Nil)
  }
}
