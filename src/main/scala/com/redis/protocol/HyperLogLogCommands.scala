package com.redis.protocol

import com.redis.serialization._


object HyperLogLogCommands {
  import DefaultWriters._

  case class PFAdd(key: String, elements: Seq[String]) extends RedisCommand[Int]("PFADD") {
    def params = key +: elements.toArgs
  }

  object PFAdd {
    def apply(key: String, element: String, elements: String*): PFAdd = PFAdd(key, element +: elements)
  }

  case class PFCount(keys: Seq[String]) extends RedisCommand[Long]("PFCOUNT") {
    def params = keys.toArgs
  }

  object PFCount {
    def apply(key: String, keys: String*): PFCount = PFCount(key +: keys)
  }

  case class PFMerge(destKey: String, sourceKeys: Seq[String]) extends RedisCommand[Boolean]("PFMERGE") {
    def params = destKey +: sourceKeys.toArgs
  }

  object PFMerge {
    def apply(destKey: String, sourceKey: String, sourceKeys: String*): PFMerge =
      PFMerge(destKey, sourceKey +: sourceKeys)
  }
}
