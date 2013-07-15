package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import HashCommands._


@RunWith(classOf[JUnitRunner])
class HashOperationsSpec extends RedisSpecBase {

   describe("hset") {
    it("should set and get fields") {
      val key = "hset1"
      hset(key, "field1", "val").apply(client)
      hget(key, "field1").apply(client).futureValue should be(Some("val"))
    }
   }

   describe("hmget") {
    it("should set and get maps") {
      val key = "hmget1"
      hmset(key, Map("field1" -> "val1", "field2" -> "val2")).apply(client)
      hmget(key, "field1").apply(client).futureValue should equal (Map("field1" -> "val1"))
      hmget(key, "field1", "field2").apply(client).futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
      hmget(key, "field1", "field2", "field3").apply(client).futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
    }
   }

   describe("hincrby") {
    it("should increment map values") {
      val key = "hincrby1"
      hincrby(key, "field1", 1).apply(client)
      hget(key, "field1").apply(client).futureValue should equal (1)
    }
   }
    
   describe("hexists") {
    it("should check existence") {
      val key = "hexists1"
      hset(key, "field1", "val").apply(client)
      hexists(key, "field1").apply(client).futureValue should be (true)
      hexists(key, "field2").apply(client).futureValue should be (false)
    }
   }
    
   describe("hdel") {
    it("should delete fields") {
      val key = "hdel1"
      hset(key, "field1", "val").apply(client)
      hexists(key, "field1").apply(client).futureValue should be (true)
      hdel(key, "field1").apply(client).futureValue should equal (1)
      hexists(key, "field1").apply(client).futureValue should be (false)
      hmset(key, Map("field1" -> "val1", "field2" -> "val2")).apply(client)
      hdel(key, "field1", "field2").apply(client).futureValue should equal (2)
    }
   }
    
   describe("hlen") {
    it("should turn the length of the fields") {
      val key = "hlen1"
      hmset(key, Map("field1" -> "val1", "field2" -> "val2")).apply(client)
      hlen(key).apply(client).futureValue should be (2)
    }
   }
    
   describe("hkeys, hvals and hgetall") {
    it("should turn the aggregates") {
      val key = "hash_aggregates"
      hmset(key, Map("field1" -> "val1", "field2" -> "val2")).apply(client)
      hkeys(key).apply(client).futureValue should equal (List("field1", "field2"))
      hvals(key).apply(client).futureValue should equal (List("val1", "val2"))
      hgetall(key).apply(client).futureValue should equal (Map("field1" -> "val1", "field2" -> "val2"))
    }
  }

}

