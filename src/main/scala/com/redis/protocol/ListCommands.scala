package com.redis.protocol

import com.redis.serialization.{Read, Write}
import RedisCommand._


object ListCommands {
  case class LPush[A](key: String, value: A, values: A*)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("LPUSH" :: key :: (value :: values.toList).map(writer.write))
  }

  case class LPushX[A](key: String, value: A)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("LPUSHX" :: key :: writer.write(value) :: Nil)
  }
  
  case class RPush[A](key: String, value: A, values: A*)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("RPUSH" :: key :: (value :: values.toList).map(writer.write))
  }

  case class RPushX[A](key: String, value: A)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("RPUSHX" :: List(key, writer.write(value)))
  }
  
  case class LRange[A](key: String, start: Int, stop: Int)(implicit reader: Read[A]) extends RedisCommand[List[A]] {
    def line = multiBulk("LRANGE" :: key :: List(start, stop).map(_.toString))
  }

  case class LLen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("LLEN" :: List(key))
  }

  case class LTrim(key: String, start: Int, end: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("LTRIM" :: key :: List(start, end).map(_.toString))
  }
  
  case class LIndex[A](key: String, index: Int)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LINDEX" :: List(key, index.toString))

  }

  case class LSet[A](key: String, index: Int, value: A)(implicit writer: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("LSET" :: List(key, index.toString, writer.write(value)))

  }

  case class LRem[A](key: String, count: Int, value: A)(implicit writer: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("LREM" :: List(key, count.toString, writer.write(value)))

  }
  
  case class LPop[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("LPOP" :: List(key))

  }
  
  case class RPop[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOP" :: List(key))

  }
  
  case class RPopLPush[A](srcKey: String, dstKey: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("RPOPLPUSH" :: List(srcKey, dstKey))

  }
  
  case class BRPopLPush[A](srcKey: String, dstKey: String, timeoutInSeconds: Int)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("BRPOPLPUSH" :: List(srcKey, dstKey, timeoutInSeconds.toString))

  }
  
  case class BLPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit reader: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BLPOP" :: key :: keys.foldRight(timeoutInSeconds.toString :: Nil)(_ :: _))

  }
  
  case class BRPop[A](timeoutInSeconds: Int, key: String, keys: String*)
                     (implicit reader: Read[A]) extends RedisCommand[Option[(String, A)]] {
    def line = multiBulk("BRPOP" :: key :: keys.foldRight(timeoutInSeconds.toString :: Nil)(_ :: _))

  }
}
