package com.redis

import scala.concurrent.Future
import scala.concurrent.duration._
import serialization._
import Parse.{Implicits => Parsers}
import RedisCommand._
import RedisReplies._
import akka.pattern.ask
import akka.actor._

object SortedSetCommands {
  case class ZAdd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk(
      "ZADD".getBytes("UTF-8") +: 
      (List(key, score, member) ::: scoreVals.toList.map(x => List(x._1, x._2)).flatten) map format.apply
    )
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class ZRem(key: Any, member: Any, members: Any*)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREM".getBytes("UTF-8") +: (key :: member :: members.toList) map format.apply)
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class ZIncrby(key: Any, incr: Double, member: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Option[Double]
    val line = multiBulk("ZINCRBY".getBytes("UTF-8") +: (Seq(key, incr, member) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk(Parse.Implicits.parseDouble)
  }
  
  case class ZCard(key: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZCARD".getBytes("UTF-8") +: (Seq(key) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
  
  case class ZScore(key: Any, element: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Option[Double]
    val line = multiBulk("ZSCORE".getBytes("UTF-8") +: (Seq(key, element) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asBulk(Parse.Implicits.parseDouble)
  }

  case class ZRange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) 
    extends SortedSetCommand {

    type Ret = List[Option[A]]
    val line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE").getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asList
  }

  case class ZRangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A])
    extends SortedSetCommand {

    type Ret = List[(A, Double)]
    val line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE").getBytes("UTF-8") +: 
      (Seq(key, start, end) map format.apply)
    )
    val ret  = RedisReply(_: Array[Byte]).asListPairs(parse, Parse.Implicits.parseDouble).flatten
  }

  case class ZRangeByScore[A](key: Any,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) extends SortedSetCommand {

    type Ret = List[Option[A]]
    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE".getBytes("UTF-8") +: (Seq(key, minParam, maxParam, limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE".getBytes("UTF-8") +: (Seq(key, maxParam, minParam, limitEntries) map format.apply)
    )
    val ret  = RedisReply(_: Array[Byte]).asList 
  }

  case class ZRangeByScoreWithScore[A](key: Any,
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)],
          sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) extends SortedSetCommand {

    type Ret = List[(A, Double)]
    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE".getBytes("UTF-8") +: (Seq(key, minParam, maxParam, "WITHSCORES", limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE".getBytes("UTF-8") +: (Seq(key, maxParam, minParam, "WITHSCORES", limitEntries) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asListPairs(parse, Parse.Implicits.parseDouble).flatten
  }

  private def zrangebyScoreWithScoreInternal[A](
          min: Double = Double.NegativeInfinity,
          minInclusive: Boolean = true,
          max: Double = Double.PositiveInfinity,
          maxInclusive: Boolean = true,
          limit: Option[(Int, Int)])
          (implicit format: Format, parse: Parse[A]): (List[Any], String, String) = {

    val limitEntries = 
      if(!limit.isEmpty) { 
        "LIMIT" :: limit.toList.flatMap(l => List(l._1, l._2))
      } else { 
        List()
      }

    val minParam = Format.formatDouble(min, minInclusive)
    val maxParam = Format.formatDouble(max, maxInclusive)
    (limitEntries, minParam, maxParam)
  }

  case class ZRank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk((if (reverse) "ZREVRANK" else "ZRANK").getBytes("UTF-8") +: (Seq(key, member) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class ZRemRangeByRank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREMRANGEBYRANK".getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class ZRemRangeByScore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREMRANGEBYSCORE".getBytes("UTF-8") +: (Seq(key, start, end) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp

  case class ZUnionInterStore(ux: setOp, dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format) 
    extends SortedSetCommand {

    type Ret = Long
    val line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE").getBytes("UTF-8") +: 
      ((Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class ZUnionInterStoreWeighted(ux: setOp, dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
    (implicit format: Format) extends SortedSetCommand {

    type Ret = Long
    val line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE").getBytes("UTF-8") +: 
      ((Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator.map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
    val ret  = RedisReply(_: Array[Byte]).asLong
  }

  case class ZCount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZCOUNT".getBytes("UTF-8") +: (List(key, Format.formatDouble(min, minInclusive), Format.formatDouble(max, maxInclusive)) map format.apply))
    val ret  = RedisReply(_: Array[Byte]).asLong
  }
}
