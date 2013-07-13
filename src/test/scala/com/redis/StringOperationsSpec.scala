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
import StringCommands._


@RunWith(classOf[JUnitRunner])
class StringOperationsSpec extends FunSpec 
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

    it("should not set values to keys already existing with option NX") {
      (set("key100", "value100") apply client) onSuccess {
        case true => 
        case _ => fail("set should pass")
      }

      val v = set("key100", "value200", Some(NX)) apply client 
      v onSuccess {
        case true => fail("an existing key with an value should not be set with NX option")
        case false => Await.result((get("key100") apply client), 3 seconds) should equal(Some("value100"))
      }
      v onFailure {
        case t => fail("set should succeed " + t)
      }
      Await.result(v, 3 seconds) should equal(false)
    }

    it("should not set values to non-existing keys with option XX") {
      val v = set("key200", "value200", Some(XX)) apply client 
      v onSuccess {
        case true => fail("set on a non existing key with XX will fail")
        case false => Await.result((get("key200") apply client), 3 seconds) should equal(None)
      }
      v onFailure {
        case t => fail("set should succeed " + t)
      }
      Await.result(v, 3 seconds) should equal(false)
    }
  }
}
