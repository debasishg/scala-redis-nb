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


trait RedisSpecBase extends FunSpec
                 with Matchers
                 with Futures
                 with ScalaFutures
                 with BeforeAndAfterEach
                 with BeforeAndAfterAll {

  implicit val executionContext = ExecutionContext.Implicits.global
  implicit val system = ActorSystem("redis-client")

  val endpoint = new InetSocketAddress("localhost", 6379)
  val client = system.actorOf(Props(new RedisClient(endpoint)), name = "redis-client")
  implicit val timeout = AkkaTimeout(5 seconds)

  override def beforeEach = {
  }

  override def afterEach = {
  }

  override def afterAll = {
    client ! "close"
    try { 
      val stopped: Future[Boolean] = gracefulStop(client, 5 seconds)
      Await.result(stopped, 5 seconds) match {
        case true => system.shutdown()
        case false => throw new Exception("client actor didn't stop properly")
      }
    } catch {
      // the actor wasn't stopped within 5 seconds
      case e: akka.pattern.AskTimeoutException => throw e
    }
  }

}

