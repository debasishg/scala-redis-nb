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

  val Newline = ByteString("\r\n")

  val NullBulkReplyCount = -1

}
