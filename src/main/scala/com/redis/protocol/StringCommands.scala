package com.redis.protocol

import scala.language.existentials
import com.redis.serialization._
import RedisCommand._


object StringCommands {
  case class Get[A](key: String)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("GET" +: key +: Nil)
  }

  sealed trait SetOption { def toSeq: Seq[String] }
  
  sealed abstract class SetExpiryOption(label: String, n: Long) extends SetOption { def toSeq = Seq(label, n.toString) }
  case class EX(expiryInSeconds: Long) extends SetExpiryOption("EX", expiryInSeconds)
  case class PX(expiryInMillis: Long) extends SetExpiryOption("PX", expiryInMillis)

  sealed abstract class SetConditionOption(label: String) extends SetOption { def toSeq = Seq(label) }
  case object NX extends SetConditionOption("NX")
  case object XX extends SetConditionOption("XX")

  case class Set(key: String, value: Stringified,
                 exORpx: Option[SetExpiryOption] = None,
                 nxORxx: Option[SetConditionOption] = None) extends RedisCommand[Boolean] {

    def line = multiBulk("SET" +: key +: value.toString +: (exORpx.toSeq ++ nxORxx.toSeq).flatMap(_.toSeq))
  }

  case class GetSet[A](key: String, value: Stringified)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("GETSET" +: key +: value.toString +: Nil)
  }
  
  case class SetNx(key: String, value: Stringified) extends RedisCommand[Boolean] {
    def line = multiBulk("SETNX" +: key +: value.toString +: Nil)
  }
  
  case class SetEx(key: String, expiry: Long, value: Stringified) extends RedisCommand[Boolean] {
    def line = multiBulk("SETEX" +: key +: expiry.toString +: value.toString +: Nil)
  }
  
  case class PSetEx(key: String, expiryInMillis: Long, value: Stringified) extends RedisCommand[Boolean] {
    def line = multiBulk("PSETEX" +: key +: expiryInMillis.toString +: value.toString +: Nil)
  }
  
  case class Incr(key: String, by: Option[Int] = None) extends RedisCommand[Long] {
    def line = multiBulk(by.fold("INCR" +: key +: Nil)("INCRBY" +: key +: _.toString +: Nil))
  }
  
  case class Decr(key: String, by: Option[Int] = None) extends RedisCommand[Long] {
    def line = multiBulk(by.fold("DECR" +: key +: Nil)("DECRBY" +: key +: _.toString +: Nil))
  }

  case class MGet[A](key: String, keys: String*)(implicit reader: Read[A])
      extends RedisCommand[Map[String, A]]()(PartialDeserializer.keyedMapPD(key +: keys)) {
    def line = multiBulk("MGET" +: key +: keys)
  }

  case class MSet(kvs: KeyValuePair*) extends RedisCommand[Boolean] {
    def line = multiBulk("MSET" +: kvs.foldRight(Seq[String]()){ case (KeyValuePair(k,v),l) => k +: v.toString +: l })
  }

  case class MSetNx(kvs: KeyValuePair*) extends RedisCommand[Boolean] {
    def line = multiBulk("MSETNX" +: kvs.foldRight(Seq[String]()){ case (KeyValuePair(k,v),l) => k +: v.toString +: l })
  }
  
  case class SetRange(key: String, offset: Int, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("SETRANGE" +: Seq(key, offset.toString, value.toString))
  }

  case class GetRange[A](key: String, start: Int, end: Int)(implicit reader: Read[A]) extends RedisCommand[Option[A]] {
    def line = multiBulk("GETRANGE" +: key +: Seq(start, end).map(_.toString))
  }
  
  case class Strlen(key: String) extends RedisCommand[Long] {
    def line = multiBulk("STRLEN" +: Seq(key))
  }
  
  case class Append(key: String, value: Stringified) extends RedisCommand[Long] {
    def line = multiBulk("APPEND" +: Seq(key, value.toString))
  }
  
  case class GetBit(key: String, offset: Int) extends RedisCommand[Boolean] {
    def line = multiBulk("GETBIT" +: Seq(key, offset.toString))
  }
  
  case class SetBit(key: String, offset: Int, value: Boolean) extends RedisCommand[Long] {
    def line = multiBulk("SETBIT" +: Seq(key, offset.toString, Write.Internal.formatBoolean(value)))
  }
  
  case class BitOp(op: String, destKey: String, srcKeys: String*) extends RedisCommand[Long] {
    def line = multiBulk("BITOP" +: op +: destKey +: srcKeys)
  }
  
  case class BitCount(key: String, range: Option[(Int, Int)]) extends RedisCommand[Long] {
    def line = multiBulk("BITCOUNT" +: key +:
      range.fold (Seq.empty[String]) { case (from, to) => Seq(from, to).map(_.toString) })
  }
}

