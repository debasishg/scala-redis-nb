package com.redis

import scala.collection.mutable.{ListBuffer, ArrayBuilder}
import akka.util.CompactByteString
import scala.annotation.tailrec
import ProtocolUtils._
import scala.language.existentials


class ResponseParser {
  import ResponseParser._

  private[this] var input: RawReply = new RawReply(CompactByteString.empty)

  def append(data: CompactByteString): Unit =
    input = input append data

  def parse(transaction: Boolean = false): ParseResult =
    try {
      val result = parseAny(transaction)
      input = input.remaining()
      ParseResult.Ok(result)
    } catch {
      case NotEnoughDataException =>
        input.rewind()
        ParseResult.NeedMoreData

      case e: Exception =>
        ParseResult.Failed(e, input.data)
    }

  def parseAny(transaction: Boolean = false): RedisReply[_] =
    input.nextByte() match {
      case Bulk => BulkReply(parseBulk())
      case Integer => IntegerReply(parseLong())
      case Status => StatusReply(parseSingle())
      case Err => ErrorReply(RedisError(parseSingle()))
      case Multi => MultiReply(parseMulti(transaction))
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

  def parseMulti(transaction: Boolean = false): List[RedisReply[_]] = {
    val buffer = new MultiReplyBuffer[RedisReply[_]]

    if (buffer.isEmpty)
      buffer.sizeHint(parseInt())

    while ( ! buffer.isDone)
      buffer += parseAny(false)

    buffer.result
  }
}

object ResponseParser {

  sealed trait ParseResult

  object ParseResult {
    case object NeedMoreData extends ParseResult
    case class Ok(reply: RedisReply[_]) extends ParseResult
    case class Failed(cause: Throwable, data: CompactByteString) extends ParseResult
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

  private class MultiReplyBuffer[T] {
    private val buffer = new ListBuffer[T]
    private var remaining: Int = _

    def isEmpty = buffer.isEmpty

    def isDone = remaining == 0

    def clear() = buffer.clear()

    def sizeHint(size: Int) = {
      remaining = size
      buffer.sizeHint(size)
    }

    def +=(elem: T) = {
      buffer += elem
      remaining -= 1
    }

    def result: List[T] = buffer.result()
    def toList = result
  }

}
