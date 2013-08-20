package com.redis.protocol

import com.redis.serialization._
import RedisCommand._


object ListCommands {
  case class LPush(key: String, value: Stringified, values: Stringified*) extends RedisCommand[Long] {
    def line = multiBulk("LPUSH" +: key +: (value +: values).map(_.toString))
  }

  case class LPushX(key: String, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("LPUSHX" +: key +: value.toString +: Nil)
  }
  
  case class RPush(key: String, value: Stringified, values: Stringified*) extends RedisCommand[Long] {
    def line = multiBulk("RPUSH" +: key +: (value +: values).map(_.toString))
  }

  case class RPushX(key: String, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("RPUSHX" +: Seq(key, value.toString))
  }
  
  case class LRange[A](key: String, start: Int, stop: Int)
                      (implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("LRANGE" +: key +: Seq(start, stop).map(_.toString))
  }

  case class LLen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("LLEN" +: Seq(key))
  }

  case class LTrim(key: String, start: Int, end: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("LTRIM" +: key +: Seq(start, end).map(_.toString))
  }
  
  case class LIndex[A](key: String, index: Int)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LINDEX" +: Seq(key, index.toString))

  }

  case class LSet(key: String, index: Int, value: Stringified) extends RedisCommand[Boolean] {
    def line = multiBulk("LSET" +: Seq(key, index.toString, value.toString))

  }

  case class LRem(key: String, count: Int, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("LREM" +: Seq(key, count.toString, value.toString))

  }
  
  case class LPop[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LPOP" +: Seq(key))

  }
  
  case class RPop[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOP" +: Seq(key))

  }
  
  case class RPopLPush[A](srcKey: String, dstKey: String)
                         (implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOPLPUSH" +: Seq(srcKey, dstKey))

  }
  
  case class BRPopLPush[A](srcKey: String, dstKey: String, timeoutInSeconds: Int)
                          (implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("BRPOPLPUSH" +: Seq(srcKey, dstKey, timeoutInSeconds.toString))

  }
  
  case class BLPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit reader: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BLPOP" +: key +: keys.foldRight(timeoutInSeconds.toString :: Nil)(_ :: _))

  }
  
  case class BRPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit reader: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BRPOP" +: key +: keys.foldRight(timeoutInSeconds.toString :: Nil)(_ :: _))

  }
}
