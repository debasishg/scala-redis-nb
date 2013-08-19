package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object ListCommands {
  case class LPush[A](key: String, value: A, values: A*)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("LPUSH" :: key :: (value :: values.toList).map(write))
  }

  case class LPushX[A](key: String, value: A)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("LPUSHX" :: key :: write(value) :: Nil)
  }
  
  case class RPush[A](key: String, value: A, values: A*)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("RPUSH" :: key :: (value :: values.toList).map(write))
  }

  case class RPushX[A](key: String, value: A)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("RPUSHX" :: List(key, write(value)))
  }
  
  case class LRange[A](key: String, start: Int, stop: Int)(implicit parse: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("LRANGE" :: key :: List(start, stop).map(_.toString))
  }

  case class LLen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("LLEN" :: List(key))
  }

  case class LTrim(key: String, start: Int, end: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("LTRIM" :: key :: List(start, end).map(_.toString))
  }
  
  case class LIndex[A](key: String, index: Int)(implicit parse: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LINDEX" :: List(key, index.toString))

  }

  case class LSet(key: String, index: Int, value: String) extends RedisCommand[Boolean] {
    def line = multiBulk("LSET" :: List(key, index.toString, value))

  }

  case class LRem(key: String, count: Int, value: String) extends RedisCommand[Long] {
    def line = multiBulk("LREM" :: List(key, count.toString, value))

  }
  
  case class LPop[A](key: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LPOP" :: List(key))

  }
  
  case class RPop[A](key: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOP" :: List(key))

  }
  
  case class RPopLPush[A](srcKey: String, dstKey: String)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOPLPUSH" :: List(srcKey, dstKey))

  }
  
  case class BRPopLPush[A](srcKey: String, dstKey: String, timeoutInSeconds: Int)(implicit read: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("BRPOPLPUSH" :: List(srcKey, dstKey, timeoutInSeconds.toString))

  }
  
  case class BLPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit read: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BLPOP" :: key :: (keys.toList :+ timeoutInSeconds.toString))

  }
  
  case class BRPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit read: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BRPOP" :: key :: (keys.toList :+ timeoutInSeconds.toString))

  }
}
