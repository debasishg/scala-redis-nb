package com.redis

import java.net.InetSocketAddress
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration._
import akka.event.Logging
import akka.util.{Timeout => AkkaTimeout}
import akka.actor._
import akka.pattern.gracefulStop

import org.scalatest._
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.time._

import api.RedisOps._

trait RedisSpecBase extends FunSpec
                 with Matchers
                 with Futures
                 with ScalaFutures
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {

  // Akka setup
  implicit val executionContext = ExecutionContext.Implicits.global
  implicit val system = ActorSystem("redis-client")
  implicit val timeout = AkkaTimeout(5 seconds)

  // Scalatest setup
  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(5, Millis))

  // Redis client setup
  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = system.actorOf(Props(new RedisClient(endpoint)), name = "redis-client")

  override def beforeEach = {
  }

  override def afterEach = {
    Await.result(flushdb apply client, 2 seconds)
  }

  override def afterAll = {

    client ! "close"
    try { 
      val stopped: Future[Boolean] = gracefulStop(client, 5 seconds)
      stopped onSuccess {
        case true => system.shutdown()
        case false => throw new Exception("client actor didn't stop properly")
      }
    } catch {
      // the actor wasn't stopped within 5 seconds
      case e: akka.pattern.AskTimeoutException => throw e
    }
  }

}

