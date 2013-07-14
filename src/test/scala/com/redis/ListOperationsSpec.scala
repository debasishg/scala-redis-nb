package com.redis

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import akka.event.Logging
import akka.util.Timeout
import akka.actor._
import ExecutionContext.Implicits.global

import api.RedisOps._

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import serialization._
import ListCommands._


@RunWith(classOf[JUnitRunner])
class ListOperationsSpec extends FunSpec
                 with ShouldMatchers
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {

  implicit val system = ActorSystem("redis-client")

  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = system.actorOf(Props(new RedisClient(endpoint)), name = "redis-client")
  implicit val timeout = Timeout(5 seconds)
  Thread.sleep(2000)

  override def beforeEach = {
  }

  override def afterEach = {
  }

  override def afterAll = {
    client ! "close"
  }

  describe("lpush") {
    it("should do an lpush and retrieve the values using lrange") {
      val forpush = List.fill(10)("listk") zip (1 to 10).map(_.toString)

      val writeListResults = forpush map { case (key, value) =>
        (key, value, (lpush(key, value) apply client))
      }

      writeListResults foreach { case (key, value, result) =>
        result onSuccess {
          case someLong: Long if someLong > 0 => {
            someLong should (be > (0L) and be <= (10L))
          }
          case _ => fail("lpush must return a positive number")
        }
      }
      writeListResults.map(e => Await.result(e._3, 3 seconds)) should equal((1 to 10).toList)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListResult = lrange[String]("listk", 0, -1) apply client
      readListResult.onSuccess {
        case result => result should equal ((1 to 10).reverse.toList)
      }
    }
  }

  describe("rpush") {
    it("should do an rpush and retrieve the values using lrange") {
      val forrpush = List.fill(10)("listr") zip (1 to 10).map(_.toString)
      val writeListRes = forrpush map { case (key, value) =>
        (key, value, rpush(key, value) apply client)
      }
      writeListRes.map(e => Await.result(e._3, 3 seconds)) should equal((1 to 10).toList)

      // do an lrange to check if they were inserted correctly & in proper order
      val readListRes = lrange[String]("listr", 0, -1) apply client
      readListRes.onSuccess {
        case result => result.reverse should equal ((1 to 10).reverse.toList)
      }
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
      Await.result(readListRes, 3 seconds) should equal(4 to 6)
    }

    it("should give an empty list when given key does not exist") {
      val readListRes = lrange[Int]("list_not_existing_key", 3, 5) apply client
      Await.result(readListRes, 3 seconds) should equal(Nil)
    }
  }

}
