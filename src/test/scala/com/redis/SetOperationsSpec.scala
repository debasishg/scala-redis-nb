package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import SetCommands._


@RunWith(classOf[JUnitRunner])
class SetOperationsSpec extends RedisSpecBase {

  describe("sadd") {
    it("should add a non-existent value to the set") {
      val key = "sadd1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
    }

    it("should not add an existing value to the set") {
      val key = "sadd2"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "foo").apply(client).futureValue should equal (0)
    }

    it("should fail if the key points to a non-set") {
      val key = "sadd3"
      lpush(key, "foo").apply(client).futureValue should equal (1)
      val thrown = evaluating { sadd(key, "foo").apply(client).futureValue } should produce [Exception]
      thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("sadd with variadic arguments") {
    it("should add a non-existent value to the set") {
      val key = "sadd_var"
      sadd(key, "foo", "bar", "baz").apply(client).futureValue should equal (3)
      sadd(key, "foo", "bar", "faz").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (0)
    }
  }

  describe("srem") {
    it("should remove a value from the set") {
      val key = "srem1"
      sadd(key, "foo", "bar", "baz").apply(client).futureValue should equal (3)
      srem(key, "bar").apply(client).futureValue should equal (1)
      srem(key, "foo").apply(client).futureValue should equal (1)
    }

    it("should not do anything if the value does not exist") {
      val key = "srem2"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      srem(key, "bar").apply(client).futureValue should equal (0)
    }

    it("should fail if the key points to a non-set") {
      val key = "srem3"
      lpush(key, "foo").apply(client).futureValue should equal (1)
      val thrown = evaluating { srem(key, "foo").apply(client).futureValue } should produce [Exception]
      thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("srem with variadic arguments") {
    it("should remove a value from the set") {
      val key = "srem_var"
      sadd(key, "foo", "bar", "baz", "faz").apply(client).futureValue should equal (4)
      srem(key, "foo", "bar").apply(client).futureValue should equal (2)
      srem(key, "foo").apply(client).futureValue should equal (0)
      srem(key, "baz", "bar").apply(client).futureValue should equal (1)
    }
  }

  describe("spop") {
    it("should pop a random element") {
      val key = "spop1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      spop(key).apply(client).futureValue should (equal (Some("foo")) or equal (Some("bar")) or equal (Some("baz")))
    }

    it("should return nil if the key does not exist") {
      val key = "spop2"
      spop(key).apply(client).futureValue should equal (None)
    }
  }

  describe("smove") {
    it("should move from one set to another") {
      val key = "smove1"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)

      sadd("set-2", "1").apply(client).futureValue should equal (1)
      sadd("set-2", "2").apply(client).futureValue should equal (1)

      smove(key, "set-2", "baz").apply(client).futureValue should equal (1)
      sadd("set-2", "baz").apply(client).futureValue should equal (0)
      sadd(key, "baz").apply(client).futureValue should equal (1)
    }

    it("should return 0 if the element does not exist in source set") {
      val key = "smove2"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      smove(key, "set-2", "bat").apply(client).futureValue should equal (0)
      smove("set-3", "set-2", "bat").apply(client).futureValue should equal (0)
    }

    it("should give error if the source or destination key is not a set") {
      val key = "smove3"

      lpush(key, "foo").apply(client).futureValue should equal (1)
      lpush(key, "bar").apply(client).futureValue should equal (2)
      lpush(key, "baz").apply(client).futureValue should equal (3)
      sadd("set-1", "foo").apply(client).futureValue should equal (1)
      val thrown = evaluating { smove(key, "set-1", "bat").apply(client).futureValue } should produce [Exception]
      thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
    }
  }

  describe("scard") {
    it("should return cardinality") {
      val key = "scard1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      scard(key).apply(client).futureValue should equal (3)
    }

    it("should return 0 if key does not exist") {
      val key = "scard2"
      scard(key).apply(client).futureValue should equal (0)
    }
  }

  describe("sismember") {
    it("should return true for membership") {
      val key = "sismember1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sismember(key, "foo").apply(client).futureValue should be (true)
    }
    
    it("should return false for no membership") {
      val key = "sismember2"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sismember(key, "fo").apply(client).futureValue should be (false)
    }

    it("should return false if key does not exist") {
      val key = "sismember3"
      sismember(key, "fo").apply(client).futureValue should be (false)
    }
  }

  describe("sinter") {
    it("should return intersection") {
      val key1 = "sinter1_1"
      val key2 = "sinter1_2"
      val key3 = "sinter1_3"

      sadd(key1, "foo").apply(client).futureValue should equal (1)
      sadd(key1, "bar").apply(client).futureValue should equal (1)
      sadd(key1, "baz").apply(client).futureValue should equal (1)

      sadd(key2, "foo").apply(client).futureValue should equal (1)
      sadd(key2, "bat").apply(client).futureValue should equal (1)
      sadd(key2, "baz").apply(client).futureValue should equal (1)

      sadd(key3, "for").apply(client).futureValue should equal (1)
      sadd(key3, "bat").apply(client).futureValue should equal (1)
      sadd(key3, "bay").apply(client).futureValue should equal (1)

      sinter(key1, key2).apply(client).futureValue should equal (Set("foo", "baz"))
      sinter(key1, key3).apply(client).futureValue should equal (Set.empty)
    }

    it("should return empty set for non-existing key") {
      val key = "sinter2"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sinter(key, "set-4").apply(client).futureValue should equal (Set()) 
    }
  }

  describe("sinterstore") {
    it("should store intersection") {
      val key1 = "sinter1_1"
      val key2 = "sinter1_2"
      val key3 = "sinter1_3"
      val keyr = "sinter1_r"
      val keys = "sinter1_s"

      sadd(key1, "foo").apply(client).futureValue should equal (1)
      sadd(key1, "bar").apply(client).futureValue should equal (1)
      sadd(key1, "baz").apply(client).futureValue should equal (1)

      sadd(key2, "foo").apply(client).futureValue should equal (1)
      sadd(key2, "bat").apply(client).futureValue should equal (1)
      sadd(key2, "baz").apply(client).futureValue should equal (1)

      sadd(key3, "for").apply(client).futureValue should equal (1)
      sadd(key3, "bat").apply(client).futureValue should equal (1)
      sadd(key3, "bay").apply(client).futureValue should equal (1)

      sinterstore(keyr, key1, key2).apply(client).futureValue should equal (2)
      scard(keyr).apply(client).futureValue should equal (2)

      sinterstore(keys, key1, key3).apply(client).futureValue should equal (0)
      scard(keys).apply(client).futureValue should equal (0)
    }

    it("should return empty set for non-existing key") {
      val key = "sinter2"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sinterstore("set-r", key, "set-4").apply(client).futureValue should equal (0)
      scard("set-r").apply(client).futureValue should equal (0)
    }
  }

  describe("sunion") {
    it("should return union") {
      val key = "sunion1"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)

      sadd("set-2", "foo").apply(client).futureValue should equal (1)
      sadd("set-2", "bat").apply(client).futureValue should equal (1)
      sadd("set-2", "baz").apply(client).futureValue should equal (1)

      sadd("set-3", "for").apply(client).futureValue should equal (1)
      sadd("set-3", "bat").apply(client).futureValue should equal (1)
      sadd("set-3", "bay").apply(client).futureValue should equal (1)

      sunion(key, "set-2").apply(client).futureValue should equal (Set("foo", "bar", "baz", "bat"))
      sunion(key, "set-3").apply(client).futureValue should equal (Set("foo", "bar", "baz", "for", "bat", "bay"))
    }

    it("should return empty set for non-existing key") {
      val key = "sunion2"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sunion(key, "set-2").apply(client).futureValue should equal (Set("foo", "bar", "baz"))
    }
  }

  describe("sunionstore") {
    it("should store union") {
      val key = "sunionstore1"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)

      sadd("set-2", "foo").apply(client).futureValue should equal (1)
      sadd("set-2", "bat").apply(client).futureValue should equal (1)
      sadd("set-2", "baz").apply(client).futureValue should equal (1)

      sadd("set-3", "for").apply(client).futureValue should equal (1)
      sadd("set-3", "bat").apply(client).futureValue should equal (1)
      sadd("set-3", "bay").apply(client).futureValue should equal (1)

      sunionstore("set-r", key, "set-2").apply(client).futureValue should equal (4)
      scard("set-r").apply(client).futureValue should equal (4)
      sunionstore("set-s", key, "set-3").apply(client).futureValue should equal (6)
      scard("set-s").apply(client).futureValue should equal (6)
    }
    it("should treat non-existing keys as empty sets") {
      val key = "sunionstore2"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sunionstore("set-r", key, "set-4").apply(client).futureValue should equal (3)
      scard("set-r").apply(client).futureValue should equal (3)
    }
  }

  describe("sdiff") {
    it("should return diff") {
      val key = "sdiff1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)

      sadd("set-2", "foo").apply(client).futureValue should equal (1)
      sadd("set-2", "bat").apply(client).futureValue should equal (1)
      sadd("set-2", "baz").apply(client).futureValue should equal (1)

      sadd("set-3", "for").apply(client).futureValue should equal (1)
      sadd("set-3", "bat").apply(client).futureValue should equal (1)
      sadd("set-3", "bay").apply(client).futureValue should equal (1)

      sdiff(key, "set-2", "set-3").apply(client).futureValue should equal (Set("bar"))
    }

    it("should treat non-existing keys as empty sets") {
      val key = "sdiff2"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      sdiff(key, "set-2").apply(client).futureValue should equal (Set("foo", "bar", "baz"))
    }
  }

  describe("smembers") {
    it("should return members of a set") {
      val key = "smembers1"
      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      smembers(key).apply(client).futureValue should equal (Set("foo", "bar", "baz"))
    }

    it("should return None for an empty set") {
      val key = "smembers2"
      smembers(key).apply(client).futureValue should equal (Set())
    }
  }

  describe("srandmember") {
    it("should return a random member") {
      val key = "srandmember1"

      sadd(key, "foo").apply(client).futureValue should equal (1)
      sadd(key, "bar").apply(client).futureValue should equal (1)
      sadd(key, "baz").apply(client).futureValue should equal (1)
      srandmember(key).apply(client).futureValue should (equal (Some("foo")) or equal (Some("bar")) or equal (Some("baz")))
    }

    it("should return None for a non-existing key") {
      val key = "srandmember2"
      srandmember(key).apply(client).futureValue should equal (None)
    }
  }

  describe("srandmember with count") {
    it("should return a list of random members") {
      val key = "srandmember_cnt1"

      sadd(key, "one").apply(client).futureValue should equal (1)
      sadd(key, "two").apply(client).futureValue should equal (1)
      sadd(key, "three").apply(client).futureValue should equal (1)
      sadd(key, "four").apply(client).futureValue should equal (1)
      sadd(key, "five").apply(client).futureValue should equal (1)
      sadd(key, "six").apply(client).futureValue should equal (1)
      sadd(key, "seven").apply(client).futureValue should equal (1)
      sadd(key, "eight").apply(client).futureValue should equal (1)

      srandmember(key, 2).apply(client).futureValue should have length (2)

      // returned elements should be unique
      val l = srandmember(key, 4).apply(client).futureValue
      l should have length (l.toSet.size)

      // returned elements may have duplicates
      srandmember(key, -4).apply(client).futureValue.size should (be <= (4))

      // if supplied count > size, then whole set is returned
      srandmember(key, 24).apply(client).futureValue.toSet.size should equal (8)
    }
  }

}


