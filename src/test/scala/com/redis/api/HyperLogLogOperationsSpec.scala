package com.redis.api

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class HyperLogLogOperationsSpec extends RedisSpecBase {

  describe("pfadd") {
    it("should add all the elements") {
      val key = "pfadd1"
      client.pfadd(key, "a", "b", "c", "d", "e").futureValue should be(1)
      client.pfcount(key).futureValue should be(5)
    }

    it("should ignore duplicated elements (with very low error rate)") {
      val key1 = "pfcount1"
      client.pfadd(key1, "foo", "bar", "zap").futureValue should be(1)
      client.pfadd(key1, "zap", "zap", "zap").futureValue should be(0)
      client.pfadd(key1, "foo", "bar").futureValue should be(0)
    }
  }

  describe("pfcount") {
    it("should return the approximated cardinality") {
      val key1 = "pfcount1"
      client.pfadd(key1, "foo", "bar", "zap").futureValue should be(1)
      client.pfcount(key1).futureValue should be(3)

      val key2 = "pfcount2"
      client.pfadd(key2, "1", "2", "3").futureValue should be(1)
      client.pfcount(key1, key2).futureValue should be(6)
    }
  }

  describe("pfmerge") {
    it("should merge multiple HyperLogLog values") {
      val key1 = "pfmerge1"
      val key2 = "pfmerge2"
      val key3 = "pfmerge3"

      client.pfadd(key1, "foo", "bar", "zap", "a").futureValue should be(1)
      client.pfadd(key2, "a", "b", "c", "foo").futureValue should be(1)

      client.pfmerge(key3, key1, key2).futureValue should be(true)

      client.pfcount(key3).futureValue should be (6)
    }
  }
}
