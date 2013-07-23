package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import serialization._


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
      client.hmget(key, "field1").futureValue should equal (Map("field1" -> "val1"))
      client.hmget(key, "field1", "field2").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
      client.hmget(key, "field1", "field2", "field3").futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
    }
   }

   describe("hincrby") {
    it("should increment map values") {
      val key = "hincrby1"
      client.hincrby(key, "field1", 1).futureValue should equal (1)
      client.hget(key, "field1").futureValue should equal (Some("1"))
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
      client.hmset(key, Map("field1" -> "val1", "field2" -> "val2"))
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

