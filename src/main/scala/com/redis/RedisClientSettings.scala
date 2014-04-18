package com.redis

import RedisClientSettings._


case class RedisClientSettings(
  backpressureBufferSettings: Option[BackpressureBufferSettings] = None,
  reconnectionSettings: Option[ReconnectionSettings] = None
)

object RedisClientSettings {

  case class BackpressureBufferSettings(lowBytes: Long, highBytes: Long, maxBytes: Long) {
    require(lowBytes >= 0, "lowWatermark needs to be non-negative")
    require(highBytes >= lowBytes, "highWatermark needs to be at least as large as lowWatermark")
    require(maxBytes >= highBytes, "maxCapacity needs to be at least as large as highWatermark")
  }

  object BackpressureBufferSettings {
    val default = BackpressureBufferSettings(lowBytes = 100, highBytes = 3000, maxBytes = 5000)
  }

  trait ReconnectionSettings {
    def newSchedule: ReconnectionSchedule

    trait ReconnectionSchedule {
      def nextDelayMs: Long
    }
  }

  case class ConstantReconnectionSettings(constantDelayMs: Long) extends ReconnectionSettings {
    require(constantDelayMs >= 0, s"Invalid negative reconnection delay (received $constantDelayMs)")

    def newSchedule: ReconnectionSchedule = new ConstantSchedule

    class ConstantSchedule extends ReconnectionSchedule {
      def nextDelayMs = constantDelayMs
    }
  }

  case class ExponentialReconnectionPolicy(baseDelayMs: Long, maxDelayMs: Long) extends ReconnectionSettings {
    require(baseDelayMs > 0, s"Base reconnection delay must be greater than 0. Received $baseDelayMs")
    require(maxDelayMs > 0, s"Maximum reconnection delay must be greater than 0. Received $maxDelayMs")
    require(maxDelayMs >= baseDelayMs, "Maximum reconnection delay cannot be smaller than base reconnection delay")

    def newSchedule = new ExponentialSchedule

    class ExponentialSchedule extends ReconnectionSchedule {
      var attempts = 0
      def nextDelayMs = {
        attempts += 1
        Math.min(baseDelayMs * (1L << attempts), maxDelayMs)
      }
    }
  }
}


