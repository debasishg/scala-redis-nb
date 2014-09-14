package com.redis.api

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class HashOperationsSpec extends RedisSpecBase {

   describe("hset") {
    it("should set and get fields") {
      val key = "hset1"
      client.hset(key, "field1", "val")
      client.hget(key, "field1").futureValue should be(Some("val"))
    }
   }

   describe("hmget") {
    it("should set and get maps") {
      val key = "hmget1"
      client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))

      client
        .hmget(key, "field1")
        .futureValue should equal (Map("field1" -> "val1"))

      client
        .hmget(key, "field1", "field2")
        .futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))

      client
        .hmget(key, "field1", "field2", "field3")
        .futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
    }
   }

   describe("hincrby") {
    it("should increment map values") {
      val key = "hincrby1"
      client.hincrby(key, "field1", 1).futureValue should equal (1)
      client.hget(key, "field1").futureValue should equal (Some("1"))
    }
   }

  describe("hincrbyfloat") {
    it("should increment the specified field value") {
      val key = "hincrbyfloat1"
      val field = "hincrbyfloatfield"
      client.hset(key, field, "10.50").futureValue should equal (true)
      client.hincrbyfloat(key, field, 1.1).futureValue should equal (Some(11.6))
      client.hget(key, field).futureValue should equal (Some("11.6"))
    }

    it("should regard the field value as 0 and increment that field if the specified field is not found") {
      val key = "hincrbyfloat2"
      val field = "hincrbyfloatfield"
      client.hincrbyfloat(key, field, 1.1).futureValue should equal (Some(1.1))
      client.hget(key, field).futureValue should equal (Some("1.1"))
    }

    it("should fail if the stored value is not a hash") {
      val key = "hincrbyfloat3"
      val field = "hincrbyfloatfield"
      client.set(key, "string").futureValue should equal (true)
      val thrown = intercept[Exception] { client.hincrbyfloat(key, field, 1.1).futureValue }
      thrown.getCause.getMessage should include ("Operation against a key holding the wrong kind of value")
    }

    it("should fail if the field value is not a number") {
      val key = "hincrbyfloat4"
      val field = "hincrbyfloatfield"
      client.hset(key, field, "non-number").futureValue should equal (true)
      val thrown = intercept[Exception] { client.hincrbyfloat(key, field, 1.1).futureValue }
      thrown.getCause.getMessage should include ("hash value is not a valid float")
    }
  }

   describe("hexists") {
    it("should check existence") {
      val key = "hexists1"
      client.hset(key, "field1", "val").futureValue should be (true)
      client.hexists(key, "field1").futureValue should be (true)
      client.hexists(key, "field2").futureValue should be (false)
    }
   }

   describe("hdel") {
    it("should delete fields") {
      val key = "hdel1"
      client.hset(key, "field1", "val")
      client.hexists(key, "field1").futureValue should be (true)
      client.hdel(key, "field1").futureValue should equal (1)
      client.hexists(key, "field1").futureValue should be (false)
      val kvs = Map("field1" -> "val1", "field2" -> "val2")
      client.hmset(key, kvs)
      client.hdel(key, "field1", "field2").futureValue should equal (2)
    }
   }

   describe("hlen") {
    it("should turn the length of the fields") {
      val key = "hlen1"
      client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))
      client.hlen(key).futureValue should be (2)
    }
   }

   describe("hkeys, hvals and client.hgetall") {
    it("should turn the aggregates") {
      val key = "hash_aggregates"
      client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))
      client.hkeys(key).futureValue should equal (List("field1", "field2"))
      client.hvals(key).futureValue should equal (List("val1", "val2"))
      client.hgetall(key).futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
    }
  }

}

