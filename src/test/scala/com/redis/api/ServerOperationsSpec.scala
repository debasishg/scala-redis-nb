package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class ServerOperationsSpec extends RedisSpecBase {

  describe("info") {
    it("should return information from the server") {
      client.info.futureValue.get.length should be > (0)
    }
  }

  describe("config") {
    it("should return appropriate config values with glob style params") {
      client.config.get("hash-max-ziplist-entries")
            .futureValue should equal(List("hash-max-ziplist-entries", "512"))

      client.config.get("*max-*-entries*")
            .futureValue should equal(List("hash-max-ziplist-entries", "512", "list-max-ziplist-entries", "512", "set-max-intset-entries", "512", "zset-max-ziplist-entries", "128"))
    }
  }
}
