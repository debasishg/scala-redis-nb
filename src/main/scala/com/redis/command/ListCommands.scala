package com.redis.command

import com.redis.serialization.{Parse, Format}
import RedisCommand._
import com.redis.RedisReply


object ListCommands {
  case class LPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSH" +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LPUSHX" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class RPush(key: Any, value: Any, values: Any*)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSH" +: (key :: value :: values.toList) map format.apply)
    val ret  = (_: RedisReply[_]).asLong
  }

  case class RPushX(key: Any, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("RPUSHX" +: (Seq(key, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class LRange[A](key: Any, start: Int, stop: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = List[A]
    val line = multiBulk("LRANGE" +: (Seq(key, start, stop) map format.apply))
    val ret  = (_: RedisReply[_]).asList[A].flatten // TODO Remove intermediate Option[A]
  }

  case class LLen(key: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LLEN" +: (Seq(format.apply(key))))
    val ret  = (_: RedisReply[_]).asLong
  }

  case class LTrim(key: Any, start: Int, end: Int)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LTRIM" +: (Seq(key, start, end) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }
  
  case class LIndex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("LINDEX" +: (Seq(key, index) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }

  case class LSet(key: Any, index: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Boolean
    val line = multiBulk("LSET" +: (Seq(key, index, value) map format.apply))
    val ret  = (_: RedisReply[_]).asBoolean
  }

  case class LRem(key: Any, count: Int, value: Any)(implicit format: Format) extends ListCommand {
    type Ret = Long
    val line = multiBulk("LREM" +: (Seq(key, count, value) map format.apply))
    val ret  = (_: RedisReply[_]).asLong
  }
  
  case class LPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("LPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }
  
  case class RPop[A](key: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("RPOP" +: (Seq(key) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }
  
  case class RPopLPush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("RPOPLPUSH" +: (Seq(srcKey, dstKey) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }
  
  case class BRPopLPush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(implicit format: Format, parse: Parse[A]) extends ListCommand {
    type Ret = Option[A]
    val line = multiBulk("BRPOPLPUSH" +: (Seq(srcKey, dstKey, timeoutInSeconds) map format.apply))
    val ret  = (_: RedisReply[_]).asBulk[A]
  }
  
  case class BLPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = Option[(K, V)]
    val line = multiBulk("BLPOP" +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs[K,V].flatten.headOption
  }
  
  case class BRPop[K, V](timeoutInSeconds: Int, key: K, keys: K*)
    (implicit format: Format, parseK: Parse[K], parseV: Parse[V]) extends ListCommand {
    type Ret = Option[(K, V)]
    val line = multiBulk("BRPOP" +: ((key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) map format.apply))
    val ret  = (_: RedisReply[_]).asListPairs[K,V].flatten.headOption
  }
}
