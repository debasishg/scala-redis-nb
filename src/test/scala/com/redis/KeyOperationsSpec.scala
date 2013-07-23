package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import serialization._


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
}


