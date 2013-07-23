package com.redis

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import akka.event.Logging
import akka.util.{Timeout => AkkaTimeout}
import akka.actor._

import org.scalatest._
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.time._
import api.RedisOps


trait RedisSpecBase extends FunSpec
                 with Matchers
                 with Futures
                 with ScalaFutures
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {

  // Akka setup
  implicit val system = ActorSystem("redis-client")
  implicit val executionContext = system.dispatcher
  implicit val timeout = AkkaTimeout(5 seconds)

  // Scalatest setup
  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  // Redis client setup
  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = RedisClient(endpoint)

  override def beforeEach = {
  }

  override def afterEach = {
    Await.result(client.flushdb(), 2 seconds)
  }

  override def afterAll =
    try { 
      client.quit() onSuccess {
        case true => system.shutdown()
        case false => throw new Exception("client actor didn't stop properly")
      }
    } catch {
      // the actor wasn't stopped within 5 seconds
      case e: akka.pattern.AskTimeoutException => throw e
    }

}

