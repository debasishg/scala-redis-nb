package com.redis.protocol

import com.redis.serialization._


object HashCommands {
  import DefaultWriters._

  case class HSet(key: String, field: String, value: Stringified) extends RedisCommand[Boolean]("HSET") {
    def params = key +: field +: value +: ANil
  }

  case class HSetNx(key: String, field: String, value: Stringified) extends RedisCommand[Boolean]("HSETNX") {
    def params = key +: field +: value +: ANil
  }

  case class HGet[A: Reader](key: String, field: String) extends RedisCommand[Option[A]]("HGET") {
    def params = key +: field +: ANil
  }
  
  case class HMSet(key: String, mapLike: Iterable[KeyValuePair]) extends RedisCommand[Boolean]("HMSET") {
    def params = key +: mapLike.foldRight(ANil) { (x, acc) => x.key +: x.value +: acc }
  }
  
  case class HMGet[A: Reader](key: String, fields: Seq[String])
      extends RedisCommand[Map[String, A]]("HMGET")(PartialDeserializer.keyedMapPD(fields)) {
    require(fields.nonEmpty, "Fields should not be empty")
    def params = key +: fields.toArgs
  }

  object HMGet {
    def apply[A: Reader](key: String, field: String, fields: String*): HMGet[A] = HMGet[A](key, field +: fields)
  }

  case class HIncrby(key: String, field: String, value: Int) extends RedisCommand[Long]("HINCRBY") {
    def params = key +: field +: value +: ANil
  }

  case class HIncrByFloat(key: String, field: String, amount: Double) extends RedisCommand[Option[Double]]("HINCRBYFLOAT") {
    def params = key +: field +: amount +: ANil
  }
  
  case class HExists(key: String, field: String) extends RedisCommand[Boolean]("HEXISTS") {
    def params = key +: field +: ANil
  }


  case class HDel(key: String, fields: Seq[String]) extends RedisCommand[Long]("HDEL") {
    require(fields.nonEmpty, "Fields should not be empty")
    def params = key +: fields.toArgs
  }

  object HDel {
    def apply(key: String, field: String, fields: String*): HDel = HDel(key, field +: fields)
  }


  case class HLen(key: String) extends RedisCommand[Long]("HLEN") {
    def params = key +: ANil
  }
  
  case class HKeys(key: String) extends RedisCommand[List[String]]("HKEYS") {
    def params = key +: ANil
  }
  
  case class HVals[A: Reader](key: String) extends RedisCommand[List[A]]("HVALS") {
    def params = key +: ANil
  }
  
  case class HGetall[A: Reader](key: String) extends RedisCommand[Map[String, A]]("HGETALL") {
    def params = key +: ANil
  }
}
