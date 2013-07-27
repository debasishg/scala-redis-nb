package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.serialization.Parse
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class ListOperationsSpec extends RedisSpecBase {

  describe("lpush") {
    it("should do an lpush and retrieve the values using lrange") {
      val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)
      val writeList = forpush map { case (key, value) => client.lpush(key, value) }
      val writeListResults = Future.sequence(writeList).futureValue

      writeListResults foreach {
        case someLong: Long if someLong > 0 =>
          someLong should (be > (0L) and be <= (10L))
        case _ => fail("lpush must return a positive number")
      }
      writeListResults should equal (1 to 10)

      // do an lrange to check if they were inserted correctly & in proper order
      val readList = client.lrange[String]("listk", 0, -1)
      readList.futureValue should equal ((1 to 10).reverse.map(_.toString))
    }
  }

  describe("rpush") {
    it("should do an rpush and retrieve the values using lrange") {
      val key = "listr"
      val forrpush = List.fill(10)(key) zip (1 to 10).map(_.toString)
      val writeList = forrpush map { case (key, value) => client.rpush(key, value) }
      val writeListResults = Future.sequence(writeList).futureValue
      writeListResults should equal (1 to 10)

      // do an lrange to check if they were inserted correctly & in proper order
      val readList = client.lrange[String](key, 0, -1)
      readList.futureValue.reverse should equal ((1 to 10).reverse.map(_.toString))
    }
  }

  import Parse.Implicits._
  describe("lrange") {
    it("should get the elements at specified offsets") {
      val key = "listlr1"
      val forrpush = List.fill(10)(key) zip (1 to 10).map(_.toString)
      val writeList = forrpush map { case (key, value) => client.rpush(key, value) }
      val writeListRes = Future.sequence(writeList).futureValue

      val readList = client.lrange[Int](key, 3, 5)
      readList.futureValue should equal (4 to 6)
    }

    it("should give an empty list when given key does not exist") {
      val readList = client.lrange[Int]("list_not_existing_key", 3, 5)
      readList.futureValue should equal (Nil)
    }
  }

}
