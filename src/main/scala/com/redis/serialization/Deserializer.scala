package com.redis.serialization

import akka.util.CompactByteString
import scala.language.existentials


class Deserializer {
  import Deserializer._
  import RawReplyReader.RawReply
  import PartialDeserializer._

  var parse: (CompactByteString, PartialFunction[RawReply, _]) => Result = parseSafe

  def parseSafe(input: CompactByteString, deserializerParts: PartialFunction[RawReply, _]): Result =
    try {
      val rawReply = new RawReply(input)
      val result = (deserializerParts orElse errorPD)(rawReply)
      Result.Ok(result, rawReply.remaining)
    } catch {
      case NotEnoughDataException =>
        parse = (i, d) => parseSafe((input ++ i).compact, d)
        Result.NeedMoreData

      case e: Throwable =>
        Result.Failed(e, input)
    }
}

object Deserializer {

  sealed trait Result

  object Result {
    case object NeedMoreData extends Result
    case class Ok[A](reply: A, remaining: CompactByteString) extends Result
    case class Failed(cause: Throwable, data: CompactByteString) extends Result
  }

  object NotEnoughDataException extends Exception
}


