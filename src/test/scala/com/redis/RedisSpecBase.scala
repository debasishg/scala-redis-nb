package com.redis

import scala.concurrent.duration._

import akka.actor._
import akka.testkit.TestKit
import akka.util.Timeout
import com.redis.RedisClientSettings.ConstantReconnectionSettings
import org.scalatest._
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.time._

class RedisSpecBase(_system: ActorSystem) extends TestKit(_system)
                 with FunSpecLike
                 with Matchers
                 with Futures
                 with ScalaFutures
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {
  // Akka setup
  def this() = this(ActorSystem("redis-test-"+ RedisSpecBase.iter.next))
  implicit val executionContext = system.dispatcher
  implicit val timeout = Timeout(2 seconds)

  // Scalatest setup
  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(5, Millis))

  // Redis client setup
  val client = RedisClient("localhost", 6379)

  def withReconnectingClient(testCode: RedisClient => Any) = {
    val client = RedisClient("localhost", 6379, settings = RedisClientSettings(reconnectionSettings = ConstantReconnectionSettings(100)))
    testCode(client)
    client.quit().futureValue should equal (true)
  }

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

  val iter = Iterator from 0

}
