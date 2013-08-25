package com.redis.serialization

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuilder
import akka.util.CompactByteString
import scala.collection.GenTraversable
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds


private[serialization] object RawReplyParser {
  import com.redis.protocol._

  def parseInt(input: RawReply): Int = {
    @tailrec def loop(acc: Int, isMinus: Boolean): Int =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }
    loop(0, false)
  }

  def parseLong(input: RawReply) = {
    @tailrec def loop(acc: Long, isMinus: Boolean): Long =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }

    loop(0, false)
  }

  // Parse a string of undetermined length
  def parseSingle(input: RawReply): String = {
    val builder = ArrayBuilder.make[Byte]

    @tailrec def loop(): Unit =
      input.nextByte() match {
        case Cr => // stop
        case b: Byte =>
          builder += b
          loop()
      }

    loop()
    input.jump(1)
    new String(builder.result)
  }

  // Parse a string with known length
  def parseBulk(input: RawReply): Option[Array[Byte]] = {
    val length = parseInt(input)

    if (length == NullBulkReplyCount) None
    else {
      val res = Some(input.take(length).toArray[Byte])
      input.jump(2)
      res
    }
  }

  def parseError(input: RawReply): RedisError =
    new RedisError(parseSingle(input))

  def parseMultiBulk[A, B[_] <: GenTraversable[_]](input: RawReply)(implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]): B[A] = {
    val builder = cbf.apply()
    val len = parseInt(input)

    @tailrec def inner(i: Int): Unit = if (i > 0) {
      builder += pd(input)
      inner(i - 1)
    }

    builder.sizeHint(len)
    inner(len)
    builder.result
  }

  class RawReply(val data: CompactByteString, private[this] var cursor: Int = 0) {
    import com.redis.serialization.Deserializer._

    def ++(other: CompactByteString) = new RawReply((data ++ other).compact, cursor)

    def hasNext = cursor < data.length

    def head =
      if (!hasNext) throw NotEnoughDataException
      else data(cursor)

    private[RawReplyParser] def nextByte() =
      if (!hasNext) throw NotEnoughDataException
      else {
        val res = data(cursor)
        cursor += 1
        res
      }

    private[RawReplyParser] def jump(amount: Int) {
      if (cursor + amount > data.length) throw NotEnoughDataException
      else cursor += amount
    }

    private[RawReplyParser] def take(amount: Int) =
      if (cursor + amount >= data.length) throw NotEnoughDataException
      else {
        val res = data.slice(cursor, cursor + amount)
        cursor += amount
        res
      }

    def remaining() = data.drop(cursor).compact
  }

  class PrefixDeserializer[A](prefix: Byte, read: RawReply => A) extends PartialDeserializer[A] {
    def isDefinedAt(x: RawReply) = x.head == prefix
    def apply(r: RawReply) = { r.jump(1); read(r) }
  }

  object PrefixDeserializer {
    val _intPD           = new PrefixDeserializer[Int]                 (Integer, parseInt _)
    val _longPD          = new PrefixDeserializer[Long]                (Integer, parseLong _)
    val _statusStringPD  = new PrefixDeserializer[String]              (Status,  parseSingle _)
    val _rawBulkPD       = new PrefixDeserializer[Option[Array[Byte]]] (Bulk,    parseBulk _)
    val _errorPD         = new PrefixDeserializer[RedisError]          (Err,     parseError _)

    val _booleanPD =
      new PrefixDeserializer[Boolean](Status, (x: RawReply) => {parseSingle(x); true }) orElse
        (_longPD andThen (_ > 0)) orElse
        (_rawBulkPD andThen (_.isDefined))

    def _multiBulkPD[A, B[_] <: GenTraversable[_]](implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]) =
      new PrefixDeserializer[B[A]](Multi, parseMultiBulk(_)(cbf, pd))
  }
}
