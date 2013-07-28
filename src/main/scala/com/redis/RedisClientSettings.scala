package com.redis

import RedisClientSettings._


case class RedisClientSettings(
  backpressureBufferSettings: Option[BackpressureBufferSettings] = None
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

}


