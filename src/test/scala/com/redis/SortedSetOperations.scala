package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import SortedSetCommands._


@RunWith(classOf[JUnitRunner])
class SortedSetOperationsSpec extends RedisSpecBase {
  import RedisCommand._
  import Parse.Implicits._

  private def add = {
    val add1 = zadd("hackers", 1965, "yukihiro matsumoto")
    val add2 = zadd("hackers", 1953, "richard stallman", (1916, "claude shannon"), (1969, "linus torvalds"), (1940, "alan kay"), (1912, "alan turing"))
    add1.apply(client).futureValue should equal (1)
    add2.apply(client).futureValue should equal (5)
  }

  describe("zadd") {
    it("should add based on proper sorted set semantics") {
      add
      zadd("hackers", 1912, "alan turing").apply(client).futureValue should equal (0)
      zcard("hackers").apply(client).futureValue should equal (6)
    }
  }

  describe("zrem") {
    it("should remove") {
      add
      zrem("hackers", "alan turing").apply(client).futureValue should equal (1)
      zrem("hackers", "alan kay", "linus torvalds").apply(client).futureValue should equal (2)
      zrem("hackers", "alan kay", "linus torvalds").apply(client).futureValue should equal (0)
    }
  }

  describe("zrange") {
    it("should get the proper range") {
      add
      zrange("hackers").apply(client).futureValue should have size (6)
      zrangeWithScore("hackers").apply(client).futureValue should have size (6)
    }
  }

  describe("zrank") {
    it ("should give proper rank") {
      add
      zrank("hackers", "yukihiro matsumoto").apply(client).futureValue should equal (4)
      zrank("hackers", "yukihiro matsumoto", reverse = true).apply(client).futureValue should equal (1)
    }
  }

  describe("zremrangebyrank") {
    it ("should remove based on rank range") {
      add
      zremrangebyrank("hackers", 0, 2).apply(client).futureValue should equal (3)
    }
  }

  describe("zremrangebyscore") {
    it ("should remove based on score range") {
      add
      zremrangebyscore("hackers", 1912, 1940).apply(client).futureValue should equal (3)
      zremrangebyscore("hackers", 0, 3).apply(client).futureValue should equal (0)
    }
  }

  describe("zunion") {
    it ("should do a union") {
      zadd("hackers 1", 1965, "yukihiro matsumoto").apply(client).futureValue should equal (1)
      zadd("hackers 1", 1953, "richard stallman").apply(client).futureValue should equal (1)
      zadd("hackers 2", 1916, "claude shannon").apply(client).futureValue should equal (1)
      zadd("hackers 2", 1969, "linus torvalds").apply(client).futureValue should equal (1)
      zadd("hackers 3", 1940, "alan kay").apply(client).futureValue should equal (1)
      zadd("hackers 4", 1912, "alan turing").apply(client).futureValue should equal (1)

      // union with weight = 1
      zunionstore("hackers", List("hackers 1", "hackers 2", "hackers 3", "hackers 4")).apply(client).futureValue should equal (6)
      zcard("hackers").apply(client).futureValue should equal (6)

      zrangeWithScore("hackers").apply(client).futureValue.map(_._2) should equal (List(1912, 1916, 1940, 1953, 1965, 1969))

      // union with modified weights
      zunionstoreweighted("hackers weighted", Map("hackers 1" -> 1.0, "hackers 2" -> 2.0, "hackers 3" -> 3.0, "hackers 4" -> 4.0)).apply(client).futureValue should equal (6)
      zrangeWithScore("hackers weighted").apply(client).futureValue.map(_._2.toInt) should equal (List(1953, 1965, 3832, 3938, 5820, 7648))
    }
  }

  describe("zinter") {
    it ("should do an intersection") {
      zadd("hackers", 1912, "alan turing").apply(client).futureValue should equal (1)
      zadd("hackers", 1916, "claude shannon").apply(client).futureValue should equal (1)
      zadd("hackers", 1927, "john mccarthy").apply(client).futureValue should equal (1)
      zadd("hackers", 1940, "alan kay").apply(client).futureValue should equal (1)
      zadd("hackers", 1953, "richard stallman").apply(client).futureValue should equal (1)
      zadd("hackers", 1954, "larry wall").apply(client).futureValue should equal (1)
      zadd("hackers", 1956, "guido van rossum").apply(client).futureValue should equal (1)
      zadd("hackers", 1965, "paul graham").apply(client).futureValue should equal (1)
      zadd("hackers", 1965, "yukihiro matsumoto").apply(client).futureValue should equal (1)
      zadd("hackers", 1969, "linus torvalds").apply(client).futureValue should equal (1)

      zadd("baby boomers", 1948, "phillip bobbit").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1953, "richard stallman").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1954, "cass sunstein").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1954, "larry wall").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1956, "guido van rossum").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1961, "lawrence lessig").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1965, "paul graham").apply(client).futureValue should equal (1)
      zadd("baby boomers", 1965, "yukihiro matsumoto").apply(client).futureValue should equal (1)

      // intersection with weight = 1
      zinterstore("baby boomer hackers", List("hackers", "baby boomers")).apply(client).futureValue should equal (5)
      zcard("baby boomer hackers").apply(client).futureValue should equal (5)

      zrange("baby boomer hackers").apply(client).futureValue should equal (List("richard stallman", "larry wall", "guido van rossum", "paul graham", "yukihiro matsumoto"))

      // intersection with modified weights
      zinterstoreweighted("baby boomer hackers weighted", Map("hackers" -> 0.5, "baby boomers" -> 0.5)).apply(client).futureValue should equal (5)
      zrangeWithScore("baby boomer hackers weighted").apply(client).futureValue.map(_._2.toInt) should equal (List(1953, 1954, 1956, 1965, 1965))
    }
  }

  describe("zcount") {
    it ("should return the number of elements between min and max") {
      add

      zcount("hackers", 1912, 1920).apply(client).futureValue should equal (2)
    }
  }

  describe("zrangeByScore") {
    it ("should return the elements between min and max") {
      add

      zrangeByScore("hackers", 1940, true, 1969, true, None).apply(client).futureValue should equal (
        List("alan kay", "richard stallman", "yukihiro matsumoto", "linus torvalds"))

      zrangeByScore("hackers", 1940, true, 1969, true, None, DESC).apply(client).futureValue should equal (
        List("linus torvalds", "yukihiro matsumoto", "richard stallman","alan kay"))
    }

    it("should return the elements between min and max and allow offset and limit") {
      add

      zrangeByScore("hackers", 1940, true, 1969, true, Some(0, 2)).apply(client).futureValue should equal (
        List("alan kay", "richard stallman"))

      zrangeByScore("hackers", 1940, true, 1969, true, Some(0, 2), DESC).apply(client).futureValue should equal (
        List("linus torvalds", "yukihiro matsumoto"))

      zrangeByScore("hackers", 1940, true, 1969, true, Some(3, 1)).apply(client).futureValue should equal (
        List("linus torvalds"))

      zrangeByScore("hackers", 1940, true, 1969, true, Some(3, 1), DESC).apply(client).futureValue should equal (
        List("alan kay"))

      zrangeByScore("hackers", 1940, false, 1969, true, Some(0, 2)).apply(client).futureValue should equal (
        List("richard stallman", "yukihiro matsumoto"))

      zrangeByScore("hackers", 1940, true, 1969, false, Some(0, 2), DESC).apply(client).futureValue should equal (
        List("yukihiro matsumoto", "richard stallman"))
    }
  }

  describe("zrangeByScoreWithScore") {
    it ("should return the elements between min and max") {
      add

      zrangeByScoreWithScore("hackers", 1940, true, 1969, true, None).apply(client).futureValue should equal (
        List(("alan kay", 1940.0), ("richard stallman", 1953.0), ("yukihiro matsumoto", 1965.0), ("linus torvalds", 1969.0)))

      zrangeByScoreWithScore("hackers", 1940, true, 1969, true, None, DESC).apply(client).futureValue should equal (
        List(("linus torvalds", 1969.0), ("yukihiro matsumoto", 1965.0), ("richard stallman", 1953.0),("alan kay", 1940.0)))

      zrangeByScoreWithScore("hackers", 1940, true, 1969, true, Some(3, 1)).apply(client).futureValue should equal (
        List(("linus torvalds", 1969.0)))

      zrangeByScoreWithScore("hackers", 1940, true, 1969, true, Some(3, 1), DESC).apply(client).futureValue should equal (
        List(("alan kay", 1940.0)))
    }
  }

}


