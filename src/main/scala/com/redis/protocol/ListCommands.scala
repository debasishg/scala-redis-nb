package com.redis.protocol

import com.redis.serialization._


object ListCommands {
  import DefaultWriters._

  case class LPush(key: String, values: Seq[Stringified]) extends RedisCommand[Long]("LPUSH") {
    require(values.nonEmpty)
    def params = key +: values.toArgs
  }

  object LPush {
    def apply(key: String, value: Stringified, values: Stringified*): LPush = LPush(key, value +: values)
  }


  case class LPushX(key: String, value: Stringified) extends RedisCommand[Long]("LPUSHX") {
    def params = key +: value +: ANil
  }


  case class RPush(key: String, values: Seq[Stringified]) extends RedisCommand[Long]("RPUSH") {
    require(values.nonEmpty)
    def params = key +: values.toArgs
  }

  object RPush {
    def apply(key: String, value: Stringified, values: Stringified*): RPush = RPush(key, value +: values)
  }


  case class RPushX(key: String, value: Stringified) extends RedisCommand[Long]("RPUSHX") {
    def params = key +: value +: ANil
  }
  
  case class LRange[A: Reader](key: String, start: Int, stop: Int) extends RedisCommand[List[A]]("LRANGE") {
    def params = key +: start +: stop +: ANil
  }

  case class LLen(key: String) extends RedisCommand[Long]("LLEN") {
    def params = key +: ANil
  }

  case class LTrim(key: String, start: Int, end: Int) extends RedisCommand[Boolean]("LTRIM") {
    def params = key +: start +: end +: ANil
  }
  
  case class LIndex[A: Reader](key: String, index: Int) extends RedisCommand[Option[A]]("LINDEX") {
    def params = key +: index +: ANil
  }

  case class LSet(key: String, index: Int, value: Stringified) extends RedisCommand[Boolean]("LSET") {
    def params = key +: index +: value +: ANil
  }

  case class LRem(key: String, count: Int, value: Stringified) extends RedisCommand[Long]("LREM") {
    def params = key +: count +: value +: ANil
  }
  
  case class LPop[A: Reader](key: String) extends RedisCommand[Option[A]]("LPOP") {
    def params = key +: ANil
  }
  
  case class RPop[A: Reader](key: String) extends RedisCommand[Option[A]]("RPOP") {
    def params = key +: ANil
  }
  
  case class RPopLPush[A: Reader](srcKey: String, dstKey: String) extends RedisCommand[Option[A]]("RPOPLPUSH") {

    def params = srcKey +: dstKey +: ANil
  }
  
  case class BRPopLPush[A: Reader](srcKey: String, dstKey: String, timeoutInSeconds: Int)
      extends RedisCommand[Option[A]]("BRPOPLPUSH") {

    def params = srcKey +: dstKey +: timeoutInSeconds +: ANil
  }
  
  case class BLPop[A: Reader](timeoutInSeconds: Int, keys: Seq[String]) extends RedisCommand[Option[(String, A)]]("BLPOP") {

    require(keys.nonEmpty)
    def params = keys.toArgs :+ timeoutInSeconds
  }

  object BLPop {
    def apply[A](timeoutInSeconds: Int, key: String, keys: String*)(implicit reader: Reader[A]): BLPop[A] =
      BLPop(timeoutInSeconds, key +: keys)
  }

  
  case class BRPop[A: Reader](timeoutInSeconds: Int, keys: Seq[String])
    extends RedisCommand[Option[(String, A)]]("BRPOP") {
    require(keys.nonEmpty)
    def params = keys.toArgs :+ timeoutInSeconds
  }

  object BRPop {
    def apply[A](timeoutInSeconds: Int, key: String, keys: String*)(implicit reader: Reader[A]): BRPop[A] =
      BRPop(timeoutInSeconds, key +: keys)
  }
}
