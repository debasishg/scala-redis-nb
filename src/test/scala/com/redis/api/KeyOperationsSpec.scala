package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class KeyOperationsSpec extends RedisSpecBase {

  describe("keys") {
    it("should fetch keys") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      val res = client.keys("anshin*")
      res.futureValue should have length (2)
    }

    it("should fetch keys with spaces") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      val res = client.keys("anshin*")
      res.futureValue should have length (2)
    }
  }

  describe("randomkey") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      val res: Future[Option[String]] = client.randomkey
      res.futureValue.get should startWith ("anshin")
    }
  }

  describe("rename") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      client.rename("anshin-2", "anshin-2-new").futureValue should be (true)
      val thrown = evaluating { client.rename("anshin-2", "anshin-2-new").futureValue } should produce[Exception]
      thrown.getCause.getMessage should equal ("ERR no such key")
    }
  }

  describe("renamenx") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      client.renamenx("anshin-2", "anshin-2-new").futureValue should be (true)
      client.renamenx("anshin-1", "anshin-2-new").futureValue should be (false)
    }
  }

  describe("dbsize") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      client.dbsize.futureValue should equal (2)
    }
  }

  describe("exists") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      client.exists("anshin-2").futureValue should be (true)
      client.exists("anshin-1").futureValue should be (true)
      client.exists("anshin-3").futureValue should be (false)
    }
  }

  describe("del") {
    it("should give") {
      val prepare = Seq(client.set("anshin-1", "debasish"), client.set("anshin-2", "maulindu"))
      val prepareRes = Future.sequence(prepare).futureValue

      client.del("anshin-2", "anshin-1").futureValue should equal (2)
      client.del("anshin-2", "anshin-1").futureValue should equal (0)
    }
  }

  describe("sort") {
    it("should give") {
      val prepare = Seq(
        client.hset("hash-1", "description", "one"),
        client.hset("hash-1", "order", "100"),
        client.hset("hash-2", "description", "two"),
        client.hset("hash-2", "order", "25"),
        client.hset("hash-3", "description", "three"),
        client.hset("hash-3", "order", "50"),
        client.sadd("alltest", 1),
        client.sadd("alltest", 2),
        client.sadd("alltest", 3)
      )
      val prepareRes = Future.sequence(prepare).futureValue

      client.sort("alltest").futureValue should equal(List("1", "2", "3"))
      client.sort("alltest", Some(Pair(0, 1))).futureValue should equal(List("1"))
      client.sort("alltest", None, true).futureValue should equal(List("3", "2", "1"))
      client.sort("alltest", None, false, false, Some("hash-*->order")).futureValue should equal(List("2", "3", "1"))
      client.sort("alltest", None, false, false, None, List("hash-*->description")).futureValue should equal(List("one", "two", "three"))
      client.sort("alltest", None, false, false, None, List("hash-*->description", "hash-*->order")).futureValue should equal(List("one", "100", "two", "25", "three", "50"))
    }
  }

  import com.redis.serialization._
  import com.redis.serialization.Parse._
  import com.redis.serialization.Parse.Implicits._

  describe("sortNStore") {
    it("should give") {
      val prepare = Seq(
        client.sadd("alltest", 10),
        client.sadd("alltest", 30),
        client.sadd("alltest", 3),
        client.sadd("alltest", 1)
      )
      val prepareRes = Future.sequence(prepare).futureValue

      // default serialization : return String
      client.sortNStore("alltest", storeAt = "skey").futureValue should equal(4)
      client.lrange("skey", 0, 10).futureValue should equal(List("1", "3", "10", "30"))

      // Long serialization : return Long
      client.sortNStore[Long]("alltest", storeAt = "skey").futureValue should equal(4)
      client.lrange[Long]("skey", 0, 10).futureValue should equal(List(1, 3, 10, 30))
    }
  }
}


