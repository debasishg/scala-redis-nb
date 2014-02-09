package com.redis.api

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class SetOperationsSpec extends RedisSpecBase {

  describe("sadd") {
    it("should add a non-existent value to the set") {
      val key = "sadd1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
    }

    it("should not add an existing value to the set") {
      val key = "sadd2"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "foo").futureValue should equal (0)
    }

    it("should fail if the key points to a non-set") {
      val key = "sadd3"
      client.lpush(key, "foo").futureValue should equal (1)
      val thrown = intercept[Exception] { client.sadd(key, "foo").futureValue }
      thrown.getCause.getMessage should equal ("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
  }

  describe("sadd with variadic arguments") {
    it("should add a non-existent value to the set") {
      val key = "sadd_var"
      client.sadd(key, "foo", "bar", "baz").futureValue should equal (3)
      client.sadd(key, "foo", "bar", "faz").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (0)
    }
  }

  describe("srem") {
    it("should remove a value from the set") {
      val key = "srem1"
      client.sadd(key, "foo", "bar", "baz").futureValue should equal (3)
      client.srem(key, "bar").futureValue should equal (1)
      client.srem(key, "foo").futureValue should equal (1)
    }

    it("should not do anything if the value does not exist") {
      val key = "srem2"
      client.sadd(key, "foo").futureValue should equal (1)
      client.srem(key, "bar").futureValue should equal (0)
    }

    it("should fail if the key points to a non-set") {
      val key = "srem3"
      client.lpush(key, "foo").futureValue should equal (1)
      val thrown = intercept[Exception] { client.srem(key, "foo").futureValue }
      thrown.getCause.getMessage should equal ("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
  }

  describe("srem with variadic arguments") {
    it("should remove a value from the set") {
      val key = "srem_var"
      client.sadd(key, "foo", "bar", "baz", "faz").futureValue should equal (4)
      client.srem(key, "foo", "bar").futureValue should equal (2)
      client.srem(key, "foo").futureValue should equal (0)
      client.srem(key, "baz", "bar").futureValue should equal (1)
    }
  }

  describe("spop") {
    it("should pop a random element") {
      val key = "spop1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)

      client
        .spop(key)
        .futureValue should (equal (Some("foo")) or equal (Some("bar")))
    }

    it("should return nil if the key does not exist") {
      val key = "spop2"
      client.spop(key).futureValue should equal (None)
    }
  }

  describe("smove") {
    it("should move from one set to another") {
      val key = "smove1"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)

      client.sadd("set-2", "1").futureValue should equal (1)
      client.sadd("set-2", "2").futureValue should equal (1)

      client.smove(key, "set-2", "baz").futureValue should equal (1)
      client.sadd("set-2", "baz").futureValue should equal (0)
      client.sadd(key, "baz").futureValue should equal (1)
    }

    it("should return 0 if the element does not exist in source set") {
      val key = "smove2"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.smove(key, "set-2", "bat").futureValue should equal (0)
      client.smove("set-3", "set-2", "bat").futureValue should equal (0)
    }

    it("should give error if the source or destination key is not a set") {
      val key = "smove3"

      client.lpush(key, "foo").futureValue should equal (1)
      client.lpush(key, "bar").futureValue should equal (2)
      client.lpush(key, "baz").futureValue should equal (3)
      client.sadd("set-1", "foo").futureValue should equal (1)
      val thrown = intercept[Exception] { client.smove(key, "set-1", "bat").futureValue }
      thrown.getCause.getMessage should equal ("WRONGTYPE Operation against a key holding the wrong kind of value")
    }
  }

  describe("scard") {
    it("should return cardinality") {
      val key = "scard1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.scard(key).futureValue should equal (3)
    }

    it("should return 0 if key does not exist") {
      val key = "scard2"
      client.scard(key).futureValue should equal (0)
    }
  }

  describe("sismember") {
    it("should return true for membership") {
      val key = "sismember1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sismember(key, "foo").futureValue should be (true)
    }

    it("should return false for no membership") {
      val key = "sismember2"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sismember(key, "fo").futureValue should be (false)
    }

    it("should return false if key does not exist") {
      val key = "sismember3"
      client.sismember(key, "fo").futureValue should be (false)
    }
  }

  describe("sinter") {
    it("should return intersection") {
      val key1 = "sinter1_1"
      val key2 = "sinter1_2"
      val key3 = "sinter1_3"

      client.sadd(key1, "foo").futureValue should equal (1)
      client.sadd(key1, "bar").futureValue should equal (1)
      client.sadd(key1, "baz").futureValue should equal (1)

      client.sadd(key2, "foo").futureValue should equal (1)
      client.sadd(key2, "bat").futureValue should equal (1)
      client.sadd(key2, "baz").futureValue should equal (1)

      client.sadd(key3, "for").futureValue should equal (1)
      client.sadd(key3, "bat").futureValue should equal (1)
      client.sadd(key3, "bay").futureValue should equal (1)

      client.sinter(key1, key2).futureValue should equal (Set("foo", "baz"))
      client.sinter(key1, key3).futureValue should equal (Set.empty)
    }

    it("should return empty set for non-existing key") {
      val key = "sinter2"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sinter(key, "set-4").futureValue should equal (Set())
    }
  }

  describe("sinterstore") {
    it("should store intersection") {
      val key1 = "sinter1_1"
      val key2 = "sinter1_2"
      val key3 = "sinter1_3"
      val keyr = "sinter1_r"
      val keys = "sinter1_s"

      client.sadd(key1, "foo").futureValue should equal (1)
      client.sadd(key1, "bar").futureValue should equal (1)
      client.sadd(key1, "baz").futureValue should equal (1)

      client.sadd(key2, "foo").futureValue should equal (1)
      client.sadd(key2, "bat").futureValue should equal (1)
      client.sadd(key2, "baz").futureValue should equal (1)

      client.sadd(key3, "for").futureValue should equal (1)
      client.sadd(key3, "bat").futureValue should equal (1)
      client.sadd(key3, "bay").futureValue should equal (1)

      client.sinterstore(keyr, key1, key2).futureValue should equal (2)
      client.scard(keyr).futureValue should equal (2)

      client.sinterstore(keys, key1, key3).futureValue should equal (0)
      client.scard(keys).futureValue should equal (0)
    }

    it("should return empty set for non-existing key") {
      val key = "sinter2"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sinterstore("set-r", key, "set-4").futureValue should equal (0)
      client.scard("set-r").futureValue should equal (0)
    }
  }

  describe("sunion") {
    it("should return union") {
      val key = "sunion1"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)

      client.sadd("set-2", "foo").futureValue should equal (1)
      client.sadd("set-2", "bat").futureValue should equal (1)
      client.sadd("set-2", "baz").futureValue should equal (1)

      client.sadd("set-3", "for").futureValue should equal (1)
      client.sadd("set-3", "bat").futureValue should equal (1)
      client.sadd("set-3", "bay").futureValue should equal (1)

      client.sunion(key, "set-2").futureValue should equal (Set("foo", "bar", "baz", "bat"))
      client.sunion(key, "set-3").futureValue should equal (Set("foo", "bar", "baz", "for", "bat", "bay"))
    }

    it("should return empty set for non-existing key") {
      val key = "sunion2"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sunion(key, "set-2").futureValue should equal (Set("foo", "bar", "baz"))
    }
  }

  describe("sunionstore") {
    it("should store union") {
      val key = "sunionstore1"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)

      client.sadd("set-2", "foo").futureValue should equal (1)
      client.sadd("set-2", "bat").futureValue should equal (1)
      client.sadd("set-2", "baz").futureValue should equal (1)

      client.sadd("set-3", "for").futureValue should equal (1)
      client.sadd("set-3", "bat").futureValue should equal (1)
      client.sadd("set-3", "bay").futureValue should equal (1)

      client.sunionstore("set-r", key, "set-2").futureValue should equal (4)
      client.scard("set-r").futureValue should equal (4)
      client.sunionstore("set-s", key, "set-3").futureValue should equal (6)
      client.scard("set-s").futureValue should equal (6)
    }
    it("should treat non-existing keys as empty sets") {
      val key = "sunionstore2"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sunionstore("set-r", key, "set-4").futureValue should equal (3)
      client.scard("set-r").futureValue should equal (3)
    }
  }

  describe("sdiff") {
    it("should return diff") {
      val key = "sdiff1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)

      client.sadd("set-2", "foo").futureValue should equal (1)
      client.sadd("set-2", "bat").futureValue should equal (1)
      client.sadd("set-2", "baz").futureValue should equal (1)

      client.sadd("set-3", "for").futureValue should equal (1)
      client.sadd("set-3", "bat").futureValue should equal (1)
      client.sadd("set-3", "bay").futureValue should equal (1)

      client.sdiff(key, "set-2", "set-3").futureValue should equal (Set("bar"))
    }

    it("should treat non-existing keys as empty sets") {
      val key = "sdiff2"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.sdiff(key, "set-2").futureValue should equal (Set("foo", "bar", "baz"))
    }
  }

  describe("smembers") {
    it("should return members of a set") {
      val key = "smembers1"
      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.smembers(key).futureValue should equal (Set("foo", "bar", "baz"))
    }

    it("should return None for an empty set") {
      val key = "smembers2"
      client.smembers(key).futureValue should equal (Set())
    }
  }

  describe("srandmember") {
    it("should return a random member") {
      val key = "srandmember1"

      client.sadd(key, "foo").futureValue should equal (1)
      client.sadd(key, "bar").futureValue should equal (1)
      client.sadd(key, "baz").futureValue should equal (1)
      client.srandmember(key).futureValue should (equal (Some("foo")) or equal (Some("bar")) or equal (Some("baz")))
    }

    it("should return None for a non-existing key") {
      val key = "srandmember2"
      client.srandmember(key).futureValue should equal (None)
    }
  }

  describe("srandmember with count") {
    it("should return a list of random members") {
      val key = "srandmember_cnt1"

      client.sadd(key, "one").futureValue should equal (1)
      client.sadd(key, "two").futureValue should equal (1)
      client.sadd(key, "three").futureValue should equal (1)
      client.sadd(key, "four").futureValue should equal (1)
      client.sadd(key, "five").futureValue should equal (1)
      client.sadd(key, "six").futureValue should equal (1)
      client.sadd(key, "seven").futureValue should equal (1)
      client.sadd(key, "eight").futureValue should equal (1)

      client.srandmember(key, 2).futureValue should have length (2)

      // returned elements should be unique
      val l = client.srandmember(key, 4).futureValue
      l should have length (l.toSet.size)

      // returned elements may have duplicates
      client.srandmember(key, -4).futureValue.size should (be <= (4))

      // if supplied count > size, then whole set is returned
      client.srandmember(key, 24).futureValue.toSet.size should equal (8)
    }
  }

}


