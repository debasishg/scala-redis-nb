package com.redis

import akka.util.ByteString
import annotation.tailrec

object ProtocolUtils {


  /**
   * Response codes from the Redis server
   */
  val Cr      = '\r'.toByte
  val Lf      = '\n'.toByte
  val Status  = '+'.toByte
  val Integer = ':'.toByte
  val Bulk    = '$'.toByte
  val Multi   = '*'.toByte
  val Err     = '-'.toByte

  val Ok      = ByteString("OK")
  val Queued  = ByteString("QUEUED")
  val LS      = ByteString("\r\n")

  val NullBulkReplyCount = -1

  /**
   * split a ByteString at a pattern of byte sequence.
   */
  def split(bytes: ByteString, delim: Seq[Byte] = Seq(Cr, Lf)): Vector[ByteString] = {

    @tailrec
    def inner(bytes: ByteString, acc: Vector[ByteString]): Vector[ByteString] =
      if (bytes.isEmpty) acc
      else {
        val (line, rest) = bytes span ( ! delim.contains(_))
        inner(rest.drop(2), acc :+ line)
      }

    inner(bytes, Vector.empty[ByteString])
  }

  /**
   * split a ByteString of multiple replies into a collection of separate replies
   * @todo : need to handle error replies
   */ 
  def splitReplies(bytes: ByteString): Vector[ByteString] = {

    def inner(bytes: ByteString, acc: Vector[ByteString]): Vector[ByteString] =
      if (bytes.isEmpty) acc
      else {
        val (head, rest) = bytes splitAt (bytes.indexOfSlice(LS) + 2)

        head(0) match {
          case Bulk =>
            val (l, a) = rest splitAt (rest.indexOfSlice(LS) + 2)
            head.drop(1).dropRight(2).utf8String.toInt match {
              case NullBulkReplyCount =>
                inner(rest, acc :+ head)

              case bodyLen =>
                val (body, realRest) = rest splitAt (bodyLen + 2)
                inner(realRest, acc :+ (head ++ body))
            }

          case Integer | Status | Err =>
            inner(rest, acc :+ head)

          case Multi =>
            val msLen = head.drop(1).dropRight(2).utf8String.toInt
            val replies = splitReplies(rest)
            val (multiReplies, others) = replies.splitAt(msLen)
            acc ++ Vector(head ++ multiReplies.fold(ByteString.empty)(_ ++ _)) ++ others
        }
      }

    inner(bytes, Vector.empty[ByteString])
  }
}
