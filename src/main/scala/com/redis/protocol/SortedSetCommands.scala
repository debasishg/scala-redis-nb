package com.redis.protocol

import com.redis.serialization.{Parse, Format}
import RedisCommand._


object SortedSetCommands {
  case class ZAdd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk(
      "ZADD" +:
      (List(key, score, member) ::: scoreVals.toList.map(x => List(x._1, x._2)).flatten) map format.apply
    )
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class ZRem(key: Any, member: Any, members: Any*)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREM" +: (key :: member :: members.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class ZIncrby(key: Any, incr: Double, member: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Option[Double]
    val line = multiBulk("ZINCRBY" +: (Seq(key, incr, member) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk(Parse.Implicits.parseDouble)
  }
  
  case class ZCard(key: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZCARD" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class ZScore(key: Any, element: Any)(implicit format: Format) extends SortedSetCommand {
    type Ret = Option[Double]
    val line = multiBulk("ZSCORE" +: (Seq(key, element) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk(Parse.Implicits.parseDouble)
  }

  case class ZRange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) 
    extends SortedSetCommand {

    type Ret = List[A]
    val line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +: (Seq(key, start, end) map format.apply))
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
  }

  case class ZRangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A])
    extends SortedSetCommand {

    type Ret = List[(A, Double)]
    val line = multiBulk(
      (if (sortAs == ASC) "ZRANGE" else "ZREVRANGE") +:
      (Seq(key, start, end, "WITHSCORES") map format.apply)
    )
    val ret  = (_: RedisReply[_]).asListPairs(parse, Parse.Implicits.parseDouble).flatten
  }

  case class ZRangeByScore[A](key: Any,
    min: Double = Double.NegativeInfinity,
    minInclusive: Boolean = true,
    max: Double = Double.PositiveInfinity,
    maxInclusive: Boolean = true,
    limit: Option[(Int, Int)],
    sortAs: SortOrder = ASC)(implicit format: Format, parse: Parse[A]) extends SortedSetCommand {

    type Ret = List[A]
    val (limitEntries, minParam, maxParam) = 
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val line = multiBulk(
      if (sortAs == ASC) "ZRANGEBYSCORE" +: ((Seq(key, minParam, maxParam) ++ limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE" +: ((Seq(key, maxParam, minParam) ++ limitEntries) map format.apply)
    )
    val ret  = (_: RedisReply[_]).asList.flatten // TODO remove intermediate Option
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
      if (sortAs == ASC) "ZRANGEBYSCORE" +: ((Seq(key, minParam, maxParam, "WITHSCORES") ++ limitEntries) map format.apply)
      else "ZREVRANGEBYSCORE" +: ((Seq(key, maxParam, minParam, "WITHSCORES") ++ limitEntries) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs(parse, Parse.Implicits.parseDouble).flatten
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
    val line = multiBulk((if (reverse) "ZREVRANK" else "ZRANK") +: (Seq(key, member) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ZRemRangeByRank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREMRANGEBYRANK" +: (Seq(key, start, end) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ZRemRangeByScore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZREMRANGEBYSCORE" +: (Seq(key, start, end) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }

  trait setOp 
  case object union extends setOp
  case object inter extends setOp

  case class ZUnionInterStore(ux: setOp, dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(implicit format: Format) 
    extends SortedSetCommand {

    type Ret = Long
    val line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
      ((Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ZUnionInterStoreWeighted(ux: setOp, dstKey: Any, kws: Iterable[Product2[Any,Double]], aggregate: Aggregate = SUM)
    (implicit format: Format) extends SortedSetCommand {

    type Ret = Long
    val line = multiBulk(
      (if (ux == union) "ZUNIONSTORE" else "ZINTERSTORE") +:
      ((Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator.map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList) map format.apply
    )
    val ret  = (_: RedisReply[_]).asLong
  }

  case class ZCount(key: Any, min: Double = Double.NegativeInfinity, max: Double = Double.PositiveInfinity, minInclusive: Boolean = true, maxInclusive: Boolean = true)(implicit format: Format) extends SortedSetCommand {
    type Ret = Long
    val line = multiBulk("ZCOUNT" +: (List(key, Format.formatDouble(min, minInclusive), Format.formatDouble(max, maxInclusive)) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
}
