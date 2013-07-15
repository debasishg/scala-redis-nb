package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import KeyCommands._


@RunWith(classOf[JUnitRunner])
class KeyOperationsSpec extends RedisSpecBase {

  describe("keys") {
    it("should fetch keys") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      val res = keys("anshin*") apply client
      res.futureValue should have length (2)
    }

    it("should fetch keys with spaces") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      val res = keys("anshin*") apply client
      res.futureValue should have length (2)
    }
  }

  describe("randomkey") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      val res: Future[Option[String]] = randomkey.apply(client)
      res.futureValue.get should startWith ("anshin")
    }
  }

  describe("rename") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      rename("anshin-2", "anshin-2-new").apply(client).futureValue should be (true)
      val thrown = evaluating { rename("anshin-2", "anshin-2-new").apply(client).futureValue } should produce[Exception]
      thrown.getCause.getMessage should equal ("ERR no such key")
    }
  }

  describe("renamenx") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      renamenx("anshin-2", "anshin-2-new").apply(client).futureValue should be (true)
      renamenx("anshin-1", "anshin-2-new").apply(client).futureValue should be (false)
    }
  }

  describe("dbsize") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      dbsize.apply(client).futureValue should equal (2)
    }
  }

  describe("exists") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      exists("anshin-2").apply(client).futureValue should be (true)
      exists("anshin-1").apply(client).futureValue should be (true)
      exists("anshin-3").apply(client).futureValue should be (false)
    }
  }

  describe("del") {
    it("should give") {
      val prepare = Seq(set("anshin-1", "debasish"), set("anshin-2", "maulindu")).map(_ apply client)
      val prepareRes = Future.sequence(prepare).futureValue

      del("anshin-2", "anshin-1").apply(client).futureValue should equal (2)
      del("anshin-2", "anshin-1").apply(client).futureValue should equal (0)
    }
  }
}


