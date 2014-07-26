package com.redis.serialization

import scala.annotation.tailrec

import akka.util.{ByteString, CompactByteString, ByteStringBuilder}
import scala.collection.GenTraversable
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds


private[serialization] object RawReplyParser {
  import com.redis.protocol._

  def parseInt(input: RawReply): Int = {
    @tailrec def loop(acc: Int, isMinus: Boolean): Int =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, isMinus = true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }
    loop(0, isMinus = false)
  }

  def parseLong(input: RawReply) = {
    @tailrec def loop(acc: Long, isMinus: Boolean): Long =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, isMinus = true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }

    loop(0, isMinus = false)
  }

  // Parse a string of undetermined length
  def parseSingle(input: RawReply): ByteString = {
    val builder = new ByteStringBuilder

    @tailrec def loop(): Unit =
      input.nextByte() match {
        case Cr => // stop
        case b: Byte =>
          builder += b
          loop()
      }

    loop()
    input.jump(1)
    builder.result()
  }

  // Parse a string with known length
  def parseBulk(input: RawReply): Option[ByteString] = {
    val length = parseInt(input)

    if (length == NullBulkReplyCount) None
    else {
      val res = Some(input.take(length))
      input.jump(2)
      res
    }
  }

  def parseError(input: RawReply): RedisError = {
    val msg = parseSingle(input).utf8String
    msg match {
      case "QUEUED" => Queued
      case _ => new RedisError(msg)
    }
  }

  def parseMultiBulk[A, B[_] <: GenTraversable[_]](input: RawReply)(implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]): B[A] = {
    val builder = cbf.apply()
    val len = parseInt(input)

    @tailrec def inner(i: Int): Unit = if (i > 0) {
      builder += pd(input)
      inner(i - 1)
    }

    builder.sizeHint(len)
    inner(len)
    builder.result()
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

    private[RawReplyParser] def jump(amount: Int): Unit = {
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
    val _statusStringPD  = new PrefixDeserializer[ByteString]          (Status,  parseSingle _)
    val _rawBulkPD       = new PrefixDeserializer[Option[ByteString]]  (Bulk,    parseBulk _)
    val _errorPD         = new PrefixDeserializer[RedisError]          (Err,     parseError _)

    private[this] val _booleanStatusPD =
      new PartialDeserializer[Boolean] {
        private val OKBytes = "+OK".getBytes("UTF-8")
        def isDefinedAt(x: RawReply): Boolean =
          (x.head == Status) && {
            val firstWord = parseSingle(x)
            // TODO: Find a better way to 'unread' data from the RawReply
            // The String reply '+OK' can be translated into a true, but there
            // are other String responses (e.g. '+QUEUED'), that should result
            // in different behaviour. So, we have to peek at the string to decide
            // whether it can be parsed as a Boolean. In any case, we have to
            // reset the reader index, since this is not the actual parsing.
            x.jump(-firstWord.size - 2)
            firstWord.corresponds(OKBytes)(_ == _)
          }

        def apply(r: RawReply): Boolean = {
          r.jump(1)
          parseSingle(r)
          true
        }
      }

    val _booleanPD =
      _booleanStatusPD orElse
        (_longPD andThen (_ > 0)) orElse
        (_rawBulkPD andThen (_.isDefined))

    def _multiBulkPD[A, B[_] <: GenTraversable[_]](implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]) =
      new PrefixDeserializer[B[A]](Multi, parseMultiBulk(_)(cbf, pd))
  }
}
