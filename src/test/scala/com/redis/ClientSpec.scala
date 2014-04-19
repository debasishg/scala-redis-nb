package com.redis

import scala.concurrent.Future

import org.scalatest.exceptions.TestFailedException
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import serialization._


@RunWith(classOf[JUnitRunner])
class ClientSpec extends RedisSpecBase {

  import DefaultFormats._

  describe("non blocking apis using futures") {
    it("get and set should be non blocking") {
      @volatile var callbackExecuted = false

      val ks = (1 to 10).map(i => s"client_key_$i")
      val kvs = ks.zip(1 to 10)

      val sets: Seq[Future[Boolean]] = kvs map {
        case (k, v) => client.set(k, v)
      }

      val setResult = Future.sequence(sets) map { r: Seq[Boolean] =>
        callbackExecuted = true
        r
      }

      callbackExecuted should be (false)
      setResult.futureValue should contain only (true)
      callbackExecuted should be (true)

      callbackExecuted = false
      val gets: Seq[Future[Option[Long]]] = ks.map { k => client.get[Long](k) }
      val getResult = Future.sequence(gets).map { rs =>
        callbackExecuted = true
        rs.flatten.sum
      }

      callbackExecuted should be (false)
      getResult.futureValue should equal (55)
      callbackExecuted should be (true)
    }

    it("should compose with sequential combinator") {
      val key = "client_key_seq"

      val res = for {
        p <- client.lpush(key, 0 to 100)
        if p > 0
        r <- client.lrange[Long](key, 0, -1)
      } yield (p, r)

      val (count, list) = res.futureValue
      count should equal (101)
      list.reverse should equal (0 to 100)
    }
  }

  describe("error handling using promise failure") {
    it("should give error trying to lpush on a key that has a non list value") {
      val key = "client_err"
      client.set(key, "value200").futureValue should be (true)

      val thrown = intercept[TestFailedException] {
        client.lpush(key, 1200).futureValue
      }

      thrown.getCause.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }

  describe("reconnections based on policy") {
    it("should reconnect") {
      val key = "reconnect_test"

      client.lpush(key, 0)

      // Extract our address
      // TODO Cleaner address extraction, perhaps in ServerOperations.client?
      val address = client.client.list().futureValue.get.toString.split(" ").head.split("=").last
      client.client.kill(address).futureValue should be (true)

      client.lpush(key, 1 to 100).futureValue should equal (100)
      val list = client.lrange[Long](key, 0, -1).futureValue

      list.size should equal (101)
      list.reverse should equal (0 to 100)
    }
  }
}
