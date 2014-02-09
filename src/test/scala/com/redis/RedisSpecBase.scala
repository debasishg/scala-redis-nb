package com.redis

import scala.concurrent.duration._
import akka.util.Timeout
import akka.actor._

import org.scalatest._
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.time._


trait RedisSpecBase extends FunSpec
                 with Matchers
                 with Futures
                 with ScalaFutures
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {
  import RedisSpecBase._

  // Akka setup
  implicit val system = ActorSystem("redis-test-"+ iter.next)
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(2 seconds)

  // Scalatest setup
  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  // Redis client setup
  val client = RedisClient("localhost", 6379)

  override def beforeEach = {
    client.flushdb()
  }

  override def afterEach = { }

  override def afterAll =
    try {
      client.flushdb()
      client.quit() onSuccess {
        case true => system.shutdown()
        case false => throw new Exception("client actor didn't stop properly")
      }
    } catch {
      // the actor wasn't stopped within 5 seconds
      case e: akka.pattern.AskTimeoutException => throw e
    }

}

object RedisSpecBase {

  private val iter = Iterator from 0

}
