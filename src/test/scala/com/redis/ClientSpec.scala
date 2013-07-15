package com.redis

import scala.concurrent.Future
import scala.util.{Either, Left, Right}

import org.scalatest.TestFailedException
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._


@RunWith(classOf[JUnitRunner])
class ClientSpec extends RedisSpecBase {

  describe("set") {
    it("should set values to keys") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val writes = keys zip values map { case (key, value) => set(key, value) apply client }

      writes foreach { _.futureValue should be (true) }
    }
  }

  describe("get") {
    it("should get results for keys set earlier") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val reads = keys map { key => get(key) apply client }

      reads zip values foreach { case (result, expectedValue) =>
        result.futureValue should equal (Some(expectedValue))
      }
      Future.sequence(reads).futureValue should equal (List(Some("value1"), Some("value2"), Some("value3")))
    }
    it("should give none for unknown keys") {
      val reads = get("key10") apply client
      reads.futureValue should equal (None)
    }
  }

  import Parse.Implicits._
  describe("non blocking apis using futures") {
    it("get and set should be non blocking") {
      val kvs = (1 to 10).map(i => s"key_$i").zip(1 to 10)
      val setResults: Seq[Future[Boolean]] = kvs map {case (k, v) =>
        set(k, v) apply client
      }
      val sr = Future.sequence(setResults)

      sr.map(x => x).futureValue.forall(_ == true) should be (true)

      val ks = (1 to 10).map(i => s"key_$i")
      val getResults = ks.map {k =>
        get[Long](k) apply client
      }

      val gr = Future.sequence(getResults)
      val result = gr.map(_.flatten.sum)

      result.futureValue should equal (55)
    }

    it("should compose with sequential combinator") {
      val values = (1 to 100).toList
      val pushResult = lpush("key", 0, values:_*) apply client
      val getResult = lrange[Long]("key", 0, -1) apply client
      
      val res = for {
        p <- pushResult.mapTo[Long]
        if p > 0
        r <- getResult.mapTo[List[Long]]
      } yield (p, r)

      val (count, list) = res.futureValue
      count should equal (101)
      list.reverse should equal (0 to 100)
    }
  }

  describe("error handling using promise failure") {
    it("should give error trying to lpush on a key that has a non list value") {
      val v = set("key300", "value200") apply client
      v.futureValue should be (true)

      val x = lpush("key300", 1200) apply client
      val thrown = evaluating { x.futureValue } should produce [TestFailedException]
      thrown.getCause.getMessage should equal ("ERR Operation against a key holding the wrong kind of value")
    }
  }
}
