package com.redis

import akka.util.ByteString
import com.redis.serialization.{Writer, Stringified}


package object protocol {

  type Args = RedisCommand.Args

  val ANil = RedisCommand.Args.empty

  implicit class StringifiedArgsOps(values: Seq[Stringified]) {
    def toArgs = new Args(values)
  }

  implicit class ArgsOps[A: Writer](values: Seq[A]) {
    def toArgs = new Args(values)
  }

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

  val Newline = ByteString("\r\n")

  val NullBulkReplyCount = -1
  val NullMultiBulkReplyCount = -1

}
