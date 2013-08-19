package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object SortedSetCommands {
  case class ZAdd[A](key: String, score: Double, member: A, scoreVals: (Double, A)*)
                    (implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk(
      "ZADD" :: key :: score.toString :: write(member)
        :: scoreVals.foldRight(List[String]())((x, acc) => x._1.toString :: write(x._2) :: acc)
    )
  }
  
  case class ZRem[A](key: String, member: A, members: A*)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("ZREM" :: key :: (member :: members.toList).map(write))
  }
  
  case class ZIncrby[A](key: String, incr: Double, member: A)(implicit write: Write[A]) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZINCRBY" :: List(key, incr.toString, write(member)))
  }
  
  case class ZCard(key: String) extends RedisCommand[Long] {
    def line = multiBulk("ZCARD" :: List(key))
  }
  
  case class ZScore[A](key: String, element: A)(implicit write: Write[A]) extends RedisCommand[Option[Double]] {
    def line = multiBulk("ZSCORE" :: List(key, write(element)))
  }

  case class ZRange[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                      (implicit read: Read[A]) extends RedisCommand[List[A]] {

    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") :: key :: List(start, end).map(_.toString))
  }

  case class ZRangeWithScore[A](key: String, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)
                               (implicit read: Read[A]) extends RedisCommand[List[(A, Double)]] {
    def line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") ::
        List(key, start.toString, end.toString, "WITHSCORES")
    )
  }

  case class ZRangeByScore[A](key: String,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit read: Read[A]) extends RedisCommand[List[A]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" :: key :: minParam :: maxParam :: limitEntries
      else "ZREVRANGEBYSCORE" :: key :: maxParam :: minParam :: limitEntries
    )
  }

  case class ZRangeByScoreWithScore[A](key: String,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit read: Read[A]) extends RedisCommand[List[(A, Double)]] {

    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    def line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" :: key :: minParam :: maxParam :: "WITHSCORES" :: limitEntries
      else "ZREVRANGEBYSCORE" :: key :: maxParam :: minParam :: "WITHSCORES" :: limitEntries
    )
  }

  private def zrangebyScoreWithScoreInternal(
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)]): (List[String], String, String) = {

    val limitEntries = limit.fold(List.empty[String])(l => "LIMIT" :: List(l._1, l._2).map(_.toString))

    val minParam = Write.Internal.formatDouble(min, minInclusive)
    val maxParam = Write.Internal.formatDouble(max, maxInclusive)
    (limitEntries, minParam, maxParam)
  }

  case class ZRank[A](key: String, member: A, reverse: Boolean = false)
                  (implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk((if (reverse) "ZREVRANK" else "ZRANK") :: List(key, write(member)))
  }

  case class ZRemRangeByRank(key: String, start: Int = 0, end: Int = -1) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYRANK" :: key :: List(start, end).map(_.toString))
  }

  case class ZRemRangeByScore(key: String,
                              start: Double = Double.NegativeInfinity,
                              end: Double = Double.PositiveInfinity) extends RedisCommand[Long] {
    def line = multiBulk("ZREMRANGEBYSCORE" :: key :: List(start, end).map(_.toString))
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp

  case class ZUnionInterStore(ux: setOp, dstKey: String, keys: Iterable[String],
                              aggregate: Aggregate = SUM) extends RedisCommand[Long] {
    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") ::
      ((Iterator(dstKey, keys.size.toString) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate.toString)).toList)
    )
  }

  case class ZUnionInterStoreWeighted(ux: setOp, dstKey: String, kws: Iterable[Product2[String, Double]],
                                      aggregate: Aggregate = SUM) extends RedisCommand[Long] {

    def line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") ::
        (Iterator(dstKey, kws.size.toString) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++
          kws.iterator.map(_._2.toString) ++ Iterator("AGGREGATE", aggregate.toString)).toList
    )
  }

  case class ZCount(key: String, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity,
                    minInclusive: Boolean = true, maxInclusive: Boolean = true) extends RedisCommand[Long] {
    def line =
      multiBulk("ZCOUNT" :: key ::
        Write.Internal.formatDouble(min, minInclusive) :: Write.Internal.formatDouble(max, maxInclusive) :: Nil)
  }
}
