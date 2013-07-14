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


@RunWith(classOf[JUnitRunner])
class ClientSpec extends FunSpec 
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

  describe("set") {
    it("should set values to keys") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val writes = keys zip values map { case (key, value) => set(key, value) apply client }

      writes foreach { _ onSuccess {
        case true => 
        case _ => fail("set should pass")
      }}
    }
  }

  describe("get") {
    it("should get results for keys set earlier") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val reads = keys map { key => get(key) apply client }

      reads zip values foreach { case (result, expectedValue) =>
        result.onSuccess {
          case resultString => resultString should equal(Some(expectedValue))
        }
        result.onFailure {
          case t => println("Got some exception " + t)
        }
      }
      reads.map(e => Await.result(e, 3 seconds)) should equal(List(Some("value1"), Some("value2"), Some("value3")))
    }
    it("should give none for unknown keys") {
      val reads = get("key10") apply client
      Await.result(reads, 3 seconds) should equal(None)
    }
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
  describe("non blocking apis using futures") {
    it("get and set should be non blocking") {
      val kvs = (1 to 10).map(i => s"key_$i").zip(1 to 10)
      val setResults: Seq[Future[Boolean]] = kvs map {case (k, v) =>
        set(k, v) apply client
      }
      val sr = Future.sequence(setResults)

      Await.result(sr.map(x => x), 2 seconds).forall(_ == true) should equal(true)

      val ks = (1 to 10).map(i => s"key_$i")
      val getResults = ks.map {k =>
        get[Long](k) apply client
      }

      val gr = Future.sequence(getResults)
      val result = gr.map(_.flatten.sum)

      Await.result(result, 2 seconds) should equal(55)
    }

    it("should compose with sequential combinator") {
      val values = (1 to 100).toList
      val pushResult = lpush("key", 0, values:_*) apply client
      val getResult = lrange[Long]("key", 0, -1) apply client
      
      val res = for {
        p <- pushResult.mapTo[Long]
        if p > 0
        r <- getResult.mapTo[List[Option[Long]]]
      } yield (p, r)

      val (count, list) = Await.result(res, 2 seconds)
      count should equal(101)
      list.reverse should equal((0 to 100).map(a => Some(a)))
    }
  }

  describe("error handling using promise failure") {
    it("should give error trying to lpush on a key that has a non list value") {
      val v = set("key200", "value200") apply client 
      Await.result(v, 3 seconds) should equal(true)

      val x = lpush("key200", 1200) apply client

      x onSuccess {
        case someLong: Long => fail("lpush should fail")
        case _ => fail("lpush must return error")
      }
      x onFailure {
        case t => {
          val thrown = evaluating { Await.result(x, 3 seconds) } should produce [Exception]
          thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
        }
      }

      val thrown = evaluating {
        Await.result(x, 3 seconds)
      } should produce [Exception]
      thrown.getMessage should equal("ERR Operation against a key holding the wrong kind of value")
    }
  }
}
