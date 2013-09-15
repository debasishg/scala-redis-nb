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
}
