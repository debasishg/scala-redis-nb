package com.redis.serialization

import akka.util.CompactByteString
import com.redis.protocol._


class RawReply(val data: CompactByteString, private[this] var cursor: Int = 0) {
  import com.redis.serialization.Deserializer._

  def ++(other: CompactByteString) = new RawReply((data ++ other).compact, cursor)

  def hasNext = cursor < data.length

  def head =
    if (!hasNext) throw NotEnoughDataException
    else data(cursor)

  private[serialization] def nextByte() =
    if (!hasNext) throw NotEnoughDataException
    else {
      val res = data(cursor)
      cursor += 1
      res
    }

  private[serialization] def jump(amount: Int) {
    if (cursor + amount > data.length) throw NotEnoughDataException
    else cursor += amount
  }

  private[serialization] def take(amount: Int) =
    if (cursor + amount >= data.length) throw NotEnoughDataException
    else {
      val res = data.slice(cursor, cursor + amount)
      cursor += amount
      res
    }

  def remaining() = data.drop(cursor).compact
}
