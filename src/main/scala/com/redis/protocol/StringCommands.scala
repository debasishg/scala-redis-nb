package com.redis.protocol

import com.redis.serialization.{PartialDeserializer, Read, Write}
import RedisCommand._


object StringCommands {
  case class Get[A](key: String)(implicit parse: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("GET" :: key :: Nil)
  }

  sealed trait SetOption { def toList: List[String] }
  
  sealed abstract class SetExpiryOption(label: String, n: Long) extends SetOption { def toList = List(label, n.toString) }
  case class EX(expiryInSeconds: Long) extends SetExpiryOption("EX", expiryInSeconds)
  case class PX(expiryInMillis: Long) extends SetExpiryOption("PX", expiryInMillis)

  sealed abstract class SetConditionOption(label: String) extends SetOption { def toList = List(label) }
  case object NX extends SetConditionOption("NX")
  case object XX extends SetConditionOption("XX")

  case class Set[A](key: String, value: A, exORpx: Option[SetExpiryOption] = None, nxORxx: Option[SetConditionOption] = None)
                   (implicit write: Write[A]) extends RedisCommand[Boolean] {

    def line = multiBulk("SET" :: key :: write(value) :: (exORpx.toList ++ nxORxx.toList).flatMap(_.toList))
  }

  case class GetSet[A, B](key: String, value: A)(implicit write: Write[A], parse: Read[B]) extends RedisCommand[Option[B]] {
    def line = multiBulk("GETSET" :: key :: write(value) :: Nil)
  }
  
  case class SetNx[A](key: String, value: A)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("SETNX" :: key :: write(value) :: Nil)
  }
  
  case class SetEx[A](key: String, expiry: Long, value: A)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("SETEX" :: key :: expiry.toString :: write(value) :: Nil)
  }
  
  case class PSetEx[A](key: String, expiryInMillis: Long, value: A)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("PSETEX" :: key :: expiryInMillis.toString :: write(value) :: Nil)
  }
  
  case class Incr(key: String, by: Option[Int] = None) extends RedisCommand[Long] {
    def line = multiBulk(by.fold("INCR" :: key :: Nil)("INCRBY" :: key :: _.toString :: Nil))
  }
  
  case class Decr(key: String, by: Option[Int] = None) extends RedisCommand[Long] {
    def line = multiBulk(by.fold("DECR" :: key :: Nil)("DECRBY" :: key :: _.toString :: Nil))
  }

  case class MGet[A](key: String, keys: String*)(implicit read: Read[A])
      extends RedisCommand[Map[String, A]]()(PartialDeserializer.keyedMapPD(key :: keys.toList)) {
    def line = multiBulk("MGET" :: key :: keys.toList)
  }

  case class MSet[A](kvs: (String, A)*)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("MSET" :: kvs.foldRight(List[String]()){ case ((k,v),l) => k :: write(v) :: l })
  }

  case class MSetNx[A](kvs: (String, A)*)(implicit write: Write[A]) extends RedisCommand[Boolean] {
    def line = multiBulk("MSETNX" :: kvs.foldRight(List[String]()){ case ((k,v),l) => k :: write(v) :: l })
  }
  
  case class SetRange[A](key: String, offset: Int, value: A)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("SETRANGE" :: List(key, offset.toString, write(value)))
  }

  case class GetRange[A](key: String, start: Int, end: Int)(implicit parse: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("GETRANGE" :: key :: List(start, end).map(_.toString))
  }
  
  case class Strlen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("STRLEN" :: List(key))
  }
  
  case class Append[A](key: String, value: A)(implicit write: Write[A]) extends RedisCommand[Long] {
    def line = multiBulk("APPEND" :: List(key, write(value)))
  }
  
  case class GetBit(key: String, offset: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("GETBIT" :: List(key, offset.toString))
  }
  
  case class SetBit(key: String, offset: Int, value: Boolean) extends RedisCommand[Long] {
    def line = multiBulk("SETBIT" :: List(key, offset.toString, Write.Internal.formatBoolean(value)))
  }
  
  case class BitOp(op: String, destKey: String, srcKeys: String*) extends RedisCommand[Long] {
    def line = multiBulk("BITOP" :: op :: destKey :: srcKeys.toList)
  }
  
  case class BitCount(key: String, range: Option[(Int, Int)]) extends RedisCommand[Long] {
    def line = multiBulk("BITCOUNT" :: key :: range.fold(List[String]())(l => List(l._1, l._2).map(_.toString)))
  }
}

