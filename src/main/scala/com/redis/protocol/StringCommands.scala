package com.redis.protocol

import scala.language.existentials
import com.redis.serialization._


object StringCommands {
  import DefaultWriters._

  case class Get[A: Reader](key: String) extends RedisCommand[Option[A]]("GET") {
    def params = key +: ANil
  }

  sealed trait SetOption { def toArgs: Args }

  sealed abstract class SetExpiryOption(label: String, n: Long) extends SetOption { def toArgs = label +: n +: ANil }
  case class EX(expiryInSeconds: Long) extends SetExpiryOption("EX", expiryInSeconds)
  case class PX(expiryInMillis: Long) extends SetExpiryOption("PX", expiryInMillis)

  sealed abstract class SetConditionOption(label: String) extends SetOption { def toArgs = label +: ANil }
  case object NX extends SetConditionOption("NX")
  case object XX extends SetConditionOption("XX")

  case class Set(key: String, value: Stringified,
                 exORpx: Option[SetExpiryOption] = None,
                 nxORxx: Option[SetConditionOption] = None) extends RedisCommand[Boolean]("SET") {

    def params = key +: value +: exORpx.fold[Seq[Stringified]](Nil)(_.toArgs.values) ++: nxORxx.fold(ANil)(_.toArgs)
  }

  object Set {

    def apply(key: String, value: Stringified, setOption: SetOption): Set =
      setOption match {
        case e: SetExpiryOption => Set(key, value, exORpx = Some(e))
        case c: SetConditionOption => Set(key, value, nxORxx = Some(c))
      }

    def apply(key: String, value: Stringified, exORpx: SetExpiryOption, nxORxx: SetConditionOption): Set =
      Set(key, value, Some(exORpx), Some(nxORxx))
  }


  case class GetSet[A: Reader](key: String, value: Stringified) extends RedisCommand[Option[A]]("GETSET") {
    def params = key +: value +: ANil
  }
  
  case class SetNx(key: String, value: Stringified) extends RedisCommand[Boolean]("SETNX") {
    def params = key +: value +: ANil
  }
  
  case class SetEx(key: String, expiry: Long, value: Stringified) extends RedisCommand[Boolean]("SETEX") {
    def params = key +: expiry +: value +: ANil
  }
  
  case class PSetEx(key: String, expiryInMillis: Long, value: Stringified) extends RedisCommand[Boolean]("PSETEX") {
    def params = key +: expiryInMillis +: value +: ANil
  }

  
  case class Incr(key: String) extends RedisCommand[Long]("INCR") {
    def params = key +: ANil
  }

  case class IncrBy(key: String, amount: Int) extends RedisCommand[Long]("INCRBY") {
    def params = key +: amount +: ANil
  }


  case class Decr(key: String) extends RedisCommand[Long]("DECR") {
    def params = key +: ANil
  }

  case class DecrBy(key: String, amount: Int) extends RedisCommand[Long]("DECRBY") {
    def params = key +: amount +: ANil
  }


  case class MGet[A: Reader](keys: Seq[String])
      extends RedisCommand[Map[String, A]]("MGET")(PartialDeserializer.keyedMapPD(keys)) {
    require(keys.nonEmpty)
    def params = keys.toArgs
  }

  object MGet {
    def apply[A: Reader](key: String, keys: String*): MGet[A] = MGet(key +: keys)
  }


  case class MSet(kvs: KeyValuePair*) extends RedisCommand[Boolean]("MSET") {
    def params = kvs.foldRight(ANil) { case (KeyValuePair(k, v), l) => k +: v +: l }
  }

  case class MSetNx(kvs: KeyValuePair*) extends RedisCommand[Boolean]("MSETNX") {
    def params = kvs.foldRight(ANil) { case (KeyValuePair(k, v), l) => k +: v +: l }
  }
  
  case class SetRange(key: String, offset: Int, value: Stringified) extends RedisCommand[Long]("SETRANGE") {
    def params = key +: offset +: value +: ANil
  }

  case class GetRange[A: Reader](key: String, start: Int, end: Int) extends RedisCommand[Option[A]]("GETRANGE") {
    def params = key +: start +: end +: ANil
  }
  
  case class Strlen(key: String) extends RedisCommand[Long]("STRLEN") {
    def params = key +: ANil
  }
  
  case class Append(key: String, value: Stringified) extends RedisCommand[Long]("APPEND") {
    def params = key +: value +: ANil
  }
  
  case class GetBit(key: String, offset: Int) extends RedisCommand[Boolean]("GETBIT") {
    def params = key +: offset +: ANil
  }
  
  case class SetBit(key: String, offset: Int, value: Boolean) extends RedisCommand[Long]("SETBIT") {
    def params = key +: offset +: (if (value) "1" else "0") +: ANil
  }


  case class BitOp(op: String, destKey: String, srcKeys: Seq[String]) extends RedisCommand[Long]("BITOP") {
    require(srcKeys.nonEmpty)
    def params = op +: destKey +: srcKeys.toArgs
  }

  object BitOp {
    def apply(op: String, destKey: String, srcKey: String, srcKeys: String*): BitOp = BitOp(op, destKey, srcKey +: srcKeys)
  }


  case class BitCount(key: String, range: Option[(Int, Int)]) extends RedisCommand[Long]("BITCOUNT") {
    def params = key +: range.fold(ANil) { case (from, to) => from +: to +: ANil }
  }

  case class BitPos(key: String, bit: Boolean, start: Option[Int], end: Option[Int]) extends RedisCommand[Long]("BITPOS") {
    require(start.isDefined || end.isEmpty)
    def params = key +: (if (bit) "1" else "0") +: start.toSeq ++: end.toSeq ++: ANil
  }
}

