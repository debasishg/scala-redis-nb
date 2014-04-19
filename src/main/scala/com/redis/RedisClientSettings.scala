package com.redis

import java.lang.{Long => JLong}

import RedisClientSettings._

case class RedisClientSettings(
  backpressureBufferSettings: Option[BackpressureBufferSettings] = None,
  reconnectionSettings: ReconnectionSettings = NoReconnectionSettings
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
      val maxAttempts: Long
      var attempts = 0

      /**
       * Gets the number of milliseconds until the next reconnection attempt.
       *
       * This method is expected to increment attempts like an iterator
       *
       * @return milliseconds until the next attempt
       */
      def nextDelayMs: Long
    }
  }

  case object NoReconnectionSettings extends ReconnectionSettings{
    def newSchedule: ReconnectionSchedule = new ReconnectionSchedule {
      val maxAttempts: Long = 0
      def nextDelayMs: Long = throw new NoSuchElementException("No delay available")
    }
  }

  case class ConstantReconnectionSettings(constantDelayMs: Long, maximumAttempts: Long = Long.MaxValue) extends ReconnectionSettings {
    require(constantDelayMs >= 0, s"Invalid negative reconnection delay (received $constantDelayMs)")
    require(maximumAttempts >= 0, s"Invalid negative maximum attempts (received $maximumAttempts)")

    def newSchedule: ReconnectionSchedule = new ConstantSchedule

    class ConstantSchedule extends ReconnectionSchedule {
      val maxAttempts = maximumAttempts
      def nextDelayMs = {
        attempts += 1
        constantDelayMs
      }
    }
  }

  case class ExponentialReconnectionSettings(baseDelayMs: Long, maxDelayMs: Long, maximumAttempts: Long = Long.MaxValue) extends ReconnectionSettings {
    require(baseDelayMs > 0, s"Base reconnection delay must be greater than 0. Received $baseDelayMs")
    require(maxDelayMs > 0, s"Maximum reconnection delay must be greater than 0. Received $maxDelayMs")
    require(maxDelayMs >= baseDelayMs, "Maximum reconnection delay cannot be smaller than base reconnection delay")

    def newSchedule = new ExponentialSchedule

    private val ceil = if ((baseDelayMs & (baseDelayMs - 1)) == 0) 0 else 1
    private val attemptCeiling = JLong.SIZE - JLong.numberOfLeadingZeros(Long.MaxValue / baseDelayMs) - ceil

    class ExponentialSchedule extends ReconnectionSchedule {
      val maxAttempts = maximumAttempts
      def nextDelayMs = {
        attempts += 1
        if (attempts > attemptCeiling) {
          maxDelayMs
        } else {
          val factor = 1L << (attempts - 1)
          Math.min(baseDelayMs * factor, maxDelayMs)
        }
      }
    }
  }
}


