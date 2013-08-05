package com.redis.serialization

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuilder, ListBuffer}


object ByteStringReader {
  import com.redis.protocol._

  def readInt(input: RawReply): Int = {
    @tailrec def loop(acc: Int, isMinus: Boolean): Int =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }
    loop(0, false)
  }

  def readLong(input: RawReply) = {
    @tailrec def loop(acc: Long, isMinus: Boolean): Long =
      input.nextByte().toChar match {
        case '\r' => input.jump(1); if (isMinus) -acc else acc
        case '-'  => loop(acc, true)
        case c    => loop((acc * 10) + c - '0', isMinus)
      }

    loop(0, false)
  }

  // Parse a string of undetermined length
  def readSingle(input: RawReply): String = {
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
  def readBulk(input: RawReply): Option[String] = {
    val length = readInt(input)

    if (length == NullBulkReplyCount) None
    else {
      val res = Some(input.take(length).utf8String)
      input.jump(2)
      res
    }
  }

  // Parse a string with known length
  def readString(input: RawReply): String = {
    val length = readInt(input)
    require(length != -1, "Non-empty bulk reply expected, but got nil")

    val res = input.take(length).utf8String
    input.jump(2)
    res
  }

  def readError(input: RawReply): RedisError =
    new RedisError(readSingle(input))

  def readMultiBulk[A](input: RawReply)(implicit pd: PartialDeserializer[A]): List[A] = {
    val buffer = new ListBuffer[A]
    val len = readInt(input)

    @tailrec def inner(i: Int): Unit = if (i > 0) {
      buffer += pd(input)
      inner(i - 1)
    }

    buffer.sizeHint(len)
    inner(len)
    buffer.result
  }
}
