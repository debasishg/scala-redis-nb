package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import api.RedisOps._
import serialization._
import ListCommands._


@RunWith(classOf[JUnitRunner])
class ListOperationsSpec extends RedisSpecBase {

  describe("lpush") {
    it("should do an lpush and retrieve the values using lrange") {
      val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)

      val writeListResults = forpush map { case (key, value) =>
        (key, value, (lpush(key, value) apply client))
      }

      writeListResults foreach { case (key, value, result) =>
        whenReady(result) {
          case someLong: Long if someLong > 0 => {
            someLong should (be > (0L) and be <= (10L))
          }
          case _ => fail("lpush must return a positive number")
        }
      }
      Future.sequence(writeListResults.map(_._3)).futureValue should equal (1 to 10)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListResult = lrange[String]("listk", 0, -1) apply client
      readListResult.futureValue should equal ((1 to 10).reverse.map(_.toString))
    }
  }

  describe("rpush") {
    it("should do an rpush and retrieve the values using lrange") {
      val key = "listr"
      val forrpush = List.fill(10)(key) zip (1 to 10).map(_.toString)
      val writeListRes = forrpush map { case (key, value) =>
        (key, value, rpush(key, value) apply client)
      }
      Future.sequence(writeListRes.map(_._3)).futureValue should equal (1 to 10)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListRes = lrange[String](key, 0, -1) apply client
      readListRes.futureValue.reverse should equal ((1 to 10).reverse.map(_.toString))
    }
  }

  import Parse.Implicits._
  describe("lrange") {
    it("should get the elements at specified offsets") {
      val key = "listlr1"
      val forrpush = List.fill(10)(key) zip (1 to 10).map(_.toString)
      val writeListRes = forrpush map { case (key, value) =>
        (key, value, rpush(key, value) apply client)
      }

      val readListRes = lrange[Int](key, 3, 5) apply client
      readListRes.futureValue should equal (4 to 6)
    }

    it("should give an empty list when given key does not exist") {
      val readListRes = lrange[Int]("list_not_existing_key", 3, 5) apply client
      readListRes.futureValue should equal (Nil)
    }
  }

}
