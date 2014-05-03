package com.redis.api

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class SortedSetOperationsSpec extends RedisSpecBase {

  private def add() = {
    val add1 = client.zadd("hackers", 1965, "yukihiro matsumoto")
    val add2 = client.zadd("hackers", 1953, "richard stallman")
    val add3 = client.zadd("hackers", (1916, "claude shannon"), (1969, "linus torvalds"))
    val add4 = client.zadd("hackers", Seq((1940, "alan kay"), (1912, "alan turing")))
    add1.futureValue should equal (1)
    add2.futureValue should equal (1)
    add3.futureValue should equal (2)
    add4.futureValue should equal (2)
  }

  private def addLex() = {
    val add1 = client.zadd("myzset", 0, "a")
    val add2 = client.zadd("myzset", 0, "b")
    val add3 = client.zadd("myzset", 0, "c")
    val add4 = client.zadd("myzset", 0, "d")
    val add5 = client.zadd("myzset", 0, "e")
    val add6 = client.zadd("myzset", 0, "f")
    val add7 = client.zadd("myzset", 0, "g")
    add1.futureValue should equal (1)
    add2.futureValue should equal (1)
    add3.futureValue should equal (1)
    add4.futureValue should equal (1)
    add5.futureValue should equal (1)
    add6.futureValue should equal (1)
    add7.futureValue should equal (1)
  }

  describe("zadd") {
    it("should add based on proper sorted set semantics") {
      add
      client.zadd("hackers", 1912, "alan turing").futureValue should equal (0)
      client.zcard("hackers").futureValue should equal (6)
    }
  }

  describe("zrem") {
    it("should remove") {
      add
      client.zrem("hackers", "alan turing").futureValue should equal (1)
      client.zrem("hackers", "alan kay", "linus torvalds").futureValue should equal (2)
      client.zrem("hackers", "alan kay", "linus torvalds").futureValue should equal (0)
    }
  }

  describe("zrange") {
    it("should get the proper range") {
      add
      client.zrange("hackers").futureValue should have size (6)
      client.zrangeWithScores("hackers").futureValue should have size (6)
    }
  }

  describe("zrevrange") {
    it("should get the proper range") {
      client.zadd("myzset", 1, "one").futureValue
      client.zadd("myzset", 2, "two").futureValue
      client.zadd("myzset", 3, "three").futureValue
      client.zrevrange("myzset", 0, -1).futureValue should equal(List("three", "two", "one"))
      client.zrevrange("myzset", 2, 3).futureValue should equal(List("one")) 
      client.zrevrange("myzset", -2, -1).futureValue should equal(List("two", "one")) 
    }
  }

  describe("zrank") {
    it ("should give proper rank") {
      add
      client.zrank("hackers", "yukihiro matsumoto").futureValue should equal (Some(4))
      client.zrevrank("hackers", "yukihiro matsumoto").futureValue should equal (Some(1))
      client.zrank("hackers", "michael jackson").futureValue should equal (None)
    }
  }

  describe("zremrangebyrank") {
    it ("should remove based on rank range") {
      add
      client.zremrangebyrank("hackers", 0, 2).futureValue should equal (3)
    }
  }

  describe("zremrangebyscore") {
    it ("should remove based on score range") {
      add
      client.zremrangebyscore("hackers", 1912, 1940).futureValue should equal (3)
      client.zremrangebyscore("hackers", 0, 3).futureValue should equal (0)
    }
  }

  describe("zunion") {
    it ("should do a union") {
      client.zadd("hackers 1", 1965, "yukihiro matsumoto").futureValue should equal (1)
      client.zadd("hackers 1", 1953, "richard stallman").futureValue should equal (1)
      client.zadd("hackers 2", 1916, "claude shannon").futureValue should equal (1)
      client.zadd("hackers 2", 1969, "linus torvalds").futureValue should equal (1)
      client.zadd("hackers 3", 1940, "alan kay").futureValue should equal (1)
      client.zadd("hackers 4", 1912, "alan turing").futureValue should equal (1)

      // union with weight = 1
      client.zunionstore("hackers", List("hackers 1", "hackers 2", "hackers 3", "hackers 4")).futureValue should equal (6)
      client.zcard("hackers").futureValue should equal (6)

      client.zrangeWithScores("hackers").futureValue.map(_._2) should equal (List(1912, 1916, 1940, 1953, 1965, 1969))

      // union with modified weights
      client.zunionstoreweighted("hackers weighted", Map("hackers 1" -> 1.0, "hackers 2" -> 2.0, "hackers 3" -> 3.0, "hackers 4" -> 4.0)).futureValue should equal (6)
      client.zrangeWithScores("hackers weighted").futureValue.map(_._2.toInt) should equal (List(1953, 1965, 3832, 3938, 5820, 7648))
    }
  }

  describe("zinter") {
    it ("should do an intersection") {
      client.zadd("hackers", 1912, "alan turing").futureValue should equal (1)
      client.zadd("hackers", 1916, "claude shannon").futureValue should equal (1)
      client.zadd("hackers", 1927, "john mccarthy").futureValue should equal (1)
      client.zadd("hackers", 1940, "alan kay").futureValue should equal (1)
      client.zadd("hackers", 1953, "richard stallman").futureValue should equal (1)
      client.zadd("hackers", 1954, "larry wall").futureValue should equal (1)
      client.zadd("hackers", 1956, "guido van rossum").futureValue should equal (1)
      client.zadd("hackers", 1965, "paul graham").futureValue should equal (1)
      client.zadd("hackers", 1965, "yukihiro matsumoto").futureValue should equal (1)
      client.zadd("hackers", 1969, "linus torvalds").futureValue should equal (1)

      client.zadd("baby boomers", 1948, "phillip bobbit").futureValue should equal (1)
      client.zadd("baby boomers", 1953, "richard stallman").futureValue should equal (1)
      client.zadd("baby boomers", 1954, "cass sunstein").futureValue should equal (1)
      client.zadd("baby boomers", 1954, "larry wall").futureValue should equal (1)
      client.zadd("baby boomers", 1956, "guido van rossum").futureValue should equal (1)
      client.zadd("baby boomers", 1961, "lawrence lessig").futureValue should equal (1)
      client.zadd("baby boomers", 1965, "paul graham").futureValue should equal (1)
      client.zadd("baby boomers", 1965, "yukihiro matsumoto").futureValue should equal (1)

      // intersection with weight = 1
      client.zinterstore("baby boomer hackers", List("hackers", "baby boomers")).futureValue should equal (5)
      client.zcard("baby boomer hackers").futureValue should equal (5)

      client.zrange("baby boomer hackers").futureValue should equal (List("richard stallman", "larry wall", "guido van rossum", "paul graham", "yukihiro matsumoto"))

      // intersection with modified weights
      client.zinterstoreweighted("baby boomer hackers weighted", Map("hackers" -> 0.5, "baby boomers" -> 0.5)).futureValue should equal (5)
      client.zrangeWithScores("baby boomer hackers weighted").futureValue.map(_._2.toInt) should equal (List(1953, 1954, 1956, 1965, 1965))
    }
  }

  describe("zcount") {
    it ("should return the number of elements between min and max") {
      add

      client.zcount("hackers", 1912, true, 1920).futureValue should equal (2)
    }
  }

  describe("z(rev)rangeByScore") {
    it ("should return the elements between min and max") {
      add

      client
        .zrangeByScore("hackers", 1940, true, 1969, true, None)
        .futureValue should equal (
          List("alan kay", "richard stallman", "yukihiro matsumoto", "linus torvalds")
        )

      client
        .zrevrangeByScore("hackers", 1940, true, 1969, true, None)
        .futureValue should equal (
          List("linus torvalds", "yukihiro matsumoto", "richard stallman","alan kay")
        )
    }

    it("should return the elements between min and max and allow offset and limit") {
      add

      client
        .zrangeByScore("hackers", 1940, true, 1969, true, Some(0, 2))
        .futureValue should equal (List("alan kay", "richard stallman"))

      client
        .zrevrangeByScore("hackers", 1940, true, 1969, true, Some(0, 2))
        .futureValue should equal (List("linus torvalds", "yukihiro matsumoto"))

      client
        .zrangeByScore("hackers", 1940, true, 1969, true, Some(3, 1))
        .futureValue should equal (List("linus torvalds"))

      client
        .zrevrangeByScore("hackers", 1940, true, 1969, true, Some(3, 1))
        .futureValue should equal (List("alan kay"))

      client
        .zrangeByScore("hackers", 1940, false, 1969, true, Some(0, 2))
        .futureValue should equal (List("richard stallman", "yukihiro matsumoto"))

      client
        .zrevrangeByScore("hackers", 1940, true, 1969, false, Some(0, 2))
        .futureValue should equal (List("yukihiro matsumoto", "richard stallman"))
    }
  }

  describe("z(rev)rangeByScoreWithScore") {
    it ("should return the elements between min and max") {
      add

      client
        .zrangeByScoreWithScores("hackers", 1940, true, 1969, true, None)
        .futureValue should equal (List(
          ("alan kay", 1940.0), ("richard stallman", 1953.0),
          ("yukihiro matsumoto", 1965.0), ("linus torvalds", 1969.0)
        ))

      client
        .zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, None)
        .futureValue should equal (List(
          ("linus torvalds", 1969.0), ("yukihiro matsumoto", 1965.0),
          ("richard stallman", 1953.0),("alan kay", 1940.0)
        ))

      client
        .zrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1))
        .futureValue should equal (List(("linus torvalds", 1969.0)))

      client
        .zrevrangeByScoreWithScores("hackers", 1940, true, 1969, true, Some(3, 1))
        .futureValue should equal (List(("alan kay", 1940.0)))
    }
  }

  describe("zlexcount") {
    it ("should return the number of elements in lexicographic ordering between min and max") {
      addLex
      client.zlexcount("myzset", "a", true, "d", true).futureValue should equal(4)
      client.zlexcount("myzset", "a", true, "d", false).futureValue should equal(3)
      client.zlexcount("myzset", "a", false, "d", true).futureValue should equal(3)
      client.zlexcount("myzset", "a", false, "d", false).futureValue should equal(2)
      client.zlexcount("myzset", maxKey = "d", maxInclusive = false).futureValue should equal(3)
      client.zlexcount("myzset", "a", false).futureValue should equal(6)
      client.zlexcount("myzset").futureValue should equal(7)
    }
  }

  describe("zrangebylex") {
    it ("should return the elements in lexicographic ordering between min and max") {
      addLex
      client.zrangebylex("myzset", "a", true, "d", true).futureValue should equal(List("a", "b", "c", "d"))
      client.zrangebylex("myzset", "a", true, "d", false).futureValue should equal(List("a", "b", "c"))
      client.zrangebylex("myzset", "a", false, "d", true).futureValue should equal(List("b", "c", "d"))
      client.zrangebylex("myzset", "a", false, "d", false).futureValue should equal(List("b", "c"))
      client.zrangebylex("myzset", maxKey = "d", maxInclusive = false).futureValue should equal(List("a", "b", "c"))
      client.zrangebylex("myzset", "a", false).futureValue should equal(List("b", "c", "d", "e", "f", "g"))
      client.zrangebylex("myzset").futureValue should equal(List("a", "b", "c", "d", "e", "f", "g"))
      client.zrangebylex("myzset", "aaa", true, "g", false).futureValue should equal(List("b", "c", "d", "e", "f"))
      client.zrangebylex("myzset", "aaa", true, "g", false, Some((1, 2))).futureValue should equal(List("c", "d"))
    }
  }

  describe("zremrangebylex") {
    it ("should return the number of elements deleted in lexicographic ordering between min and max") {
      client.zadd("myzset", Seq((0, "aaaa"), (0, "b"), (0, "c"), (0, "d"), (0, "e"))).futureValue should equal(5)
      client.zadd("myzset", Seq((0, "foo"), (0, "zap"), (0, "zip"), (0, "ALPHA"), (0, "alpha"))).futureValue should equal(5)
      client.zremrangebylex("myzset", "alpha", true, "omega", true).futureValue should equal(6)
    }
  }

}
