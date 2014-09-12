package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.RedisSpecBase
import com.redis.protocol.ListCommands.{After, Before}


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

  import com.redis.serialization.DefaultFormats._

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

  describe("linsert") {
    it("should insert the value before pivot") {
      val key = "linsert1"
      client.rpush(key, "foo", "bar", "baz").futureValue should equal (3)
      client.linsert(key, Before, "bar", "mofu").futureValue should equal (4)
      client.lrange[String](key, 0, -1).futureValue should equal ("foo" :: "mofu" :: "bar" :: "baz" :: Nil)
    }

    it("should insert the value after pivot") {
      val key = "linsert2"
      client.rpush(key, "foo", "bar", "baz").futureValue should equal (3)
      client.linsert(key, After, "bar", "mofu").futureValue should equal (4)
      client.lrange[String](key, 0, -1).futureValue should equal ("foo" :: "bar" :: "mofu" :: "baz" :: Nil)
    }

    it("should not insert any value and return -1 when pivot is not found") {
      val key = "linsert3"
      client.rpush(key, "foo", "bar", "baz").futureValue should equal (3)
      client.linsert(key, Before, "mofu", "value").futureValue should equal (-1)
      client.lrange[String](key, 0, -1).futureValue should equal ("foo" :: "bar" :: "baz" :: Nil)
    }

    it("should not insert any value and return 0 when key does not exist") {
      val key = "linsert4"
      client.linsert(key, Before, "mofu", "value").futureValue should equal (0)
      client.lrange[String](key, 0, -1).futureValue should equal (Nil)
    }

    it("should fail if the key points to a non-list") {
      val key = "linsert5"
      client.set(key, "string").futureValue should equal (true)
      val thrown = intercept[Exception] { client.linsert(key, Before, "mofu", "value").futureValue }
      thrown.getCause.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }
}
