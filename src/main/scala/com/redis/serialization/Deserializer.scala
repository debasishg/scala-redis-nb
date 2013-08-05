package com.redis.serialization

import akka.util.CompactByteString
import scala.annotation.tailrec
import scala.collection.mutable.{ListBuffer, ArrayBuilder}
import scala.language.existentials
import com.redis.protocol._


class Deserializer {
  import Deserializer._

  private[this] var input: RawReply = new RawReply(CompactByteString.empty)

  def append(data: CompactByteString): Unit =
    input = input append data

  def parse(): Result =
    try {
      val result = parseReply()
      input = input.remaining()
      Result.Ok(result)
    } catch {
      case NotEnoughDataException =>
        input.rewind()
        Result.NeedMoreData

      case e: Exception =>
        Result.Failed(e, input.data)
    }

  def parseReply(): RedisReply[_] =
    input.nextByte() match {
      case Bulk => BulkReply(parseBulk())
      case Integer => IntegerReply(parseLong())
      case Status => StatusReply(parseSingle())
      case Err => ErrorReply(RedisError(parseSingle()))
      case Multi => MultiBulkReply(parseMultiBulk())
      case x => throw new IllegalArgumentException("Unexpected input: "+ x)
    }

  @tailrec final def parseInt(acc: Int = 0, isMinus: Boolean = false): Int =
    input.nextByte().toChar match {
      case '\r' => input.jump(1); if (isMinus) -acc else acc
      case '-'  => parseInt(acc, true)
      case c    => parseInt((acc * 10) + c - '0', isMinus)
    }

  @tailrec final def parseLong(acc: Long = 0, isMinus: Boolean = false): Long =
    input.nextByte().toChar match {
      case '\r' => input.jump(1); if (isMinus) -acc else acc
      case '-'  => parseLong(acc, true)
      case c    => parseLong((acc * 10) + c - '0', isMinus)
    }

  // Parse a string of undetermined length
  def parseSingle(): String = {
    val builder = ArrayBuilder.make[Byte]

    @tailrec def inner(): Unit =
      input.nextByte() match {
        case Cr => // stop
        case b: Byte =>
          builder += b
          inner()
      }

    inner()
    input.jump(1)
    new String(builder.result)
  }

  // Parse a string with known length
  def parseBulk(): Option[String] = {
    val length = parseInt()

    if (length == NullBulkReplyCount) None
    else {
      val res = Some(input.take(length).utf8String)
      input.jump(2)
      res
    }
  }

  def parseMultiBulk(): List[RedisReply[_]] = {
    val buffer = new ListBuffer[RedisReply[_]]
    val len = parseInt()

    @tailrec def inner(i: Int): Unit = if (i > 0) {
      val a = parseReply()
      buffer += a
      inner(i - 1)
    }

    buffer.sizeHint(len)
    inner(len)
    buffer.result
  }
}

object Deserializer {

  sealed trait Result

  object Result {
    case object NeedMoreData extends Result
    case class Ok(reply: RedisReply[_]) extends Result
    case class Failed(cause: Throwable, data: CompactByteString) extends Result
  }

  object NotEnoughDataException extends Exception


  private class RawReply(
        val data: CompactByteString,
        private[this] var cursor: Int = 0
      ) {

    def append(other: CompactByteString) = new RawReply((data ++ other).compact, cursor)

    def hasNext = cursor < data.length

    def nextByte() =
      if (!hasNext) throw NotEnoughDataException
      else {
        val res = data(cursor)
        cursor += 1
        res
      }

    def jump(amount: Int) {
      if (cursor + amount > data.length) throw NotEnoughDataException
      else cursor += amount
    }

    def take(amount: Int) =
      if (cursor + amount >= data.length) throw NotEnoughDataException
      else {
        val res = data.slice(cursor, cursor + amount)
        cursor += amount
        res
      }

    def rewind() = {
      while (cursor > 0 && cursor >= data.length && data(cursor) != Lf) cursor -= 1
      if (data.nonEmpty && data(cursor) == Lf) cursor += 1
    }

    def remaining() = new RawReply(data.drop(cursor).compact, 0)
  }

}
