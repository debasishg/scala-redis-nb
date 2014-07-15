package com.redis

import scala.concurrent.Future

import akka.testkit.TestProbe
import org.junit.runner.RunWith
import org.scalatest.exceptions.TestFailedException
import org.scalatest.junit.JUnitRunner
import serialization._
import akka.io.Tcp.{Connected, CommandFailed}
import scala.reflect.ClassTag
import scala.concurrent.duration._
import com.redis.RedisClientSettings.ConstantReconnectionSettings
import com.redis.protocol.ServerCommands.Client.Kill

@RunWith(classOf[JUnitRunner])
class ClientSpec extends RedisSpecBase {

  import DefaultFormats._

  describe("non blocking apis using futures") {
    it("get and set should be non blocking") {
      @volatile var callbackExecuted = false

      val ks = (1 to 10).map(i => s"client_key_$i")
      val kvs = ks.zip(1 to 10)

      val sets: Seq[Future[Boolean]] = kvs map {
        case (k, v) => client.set(k, v)
      }

      val setResult = Future.sequence(sets) map { r: Seq[Boolean] =>
        callbackExecuted = true
        r
      }

      callbackExecuted should be (false)
      setResult.futureValue should contain only (true)
      callbackExecuted should be (true)

      callbackExecuted = false
      val gets: Seq[Future[Option[Long]]] = ks.map { k => client.get[Long](k) }
      val getResult = Future.sequence(gets).map { rs =>
        callbackExecuted = true
        rs.flatten.sum
      }

      callbackExecuted should be (false)
      getResult.futureValue should equal (55)
      callbackExecuted should be (true)
    }

    it("should compose with sequential combinator") {
      val key = "client_key_seq"

      val res = for {
        p <- client.lpush(key, 0 to 100)
        if p > 0
        r <- client.lrange[Long](key, 0, -1)
      } yield (p, r)

      val (count, list) = res.futureValue
      count should equal (101)
      list.reverse should equal (0 to 100)
    }
  }

  describe("error handling using promise failure") {
    it("should give error trying to lpush on a key that has a non list value") {
      val key = "client_err"
      client.set(key, "value200").futureValue should be (true)

      val thrown = intercept[TestFailedException] {
        client.lpush(key, 1200).futureValue
      }

      thrown.getCause.getMessage should include ("Operation against a key holding the wrong kind of value")
    }
  }

  describe("reconnections based on policy") {

    // Since 2.8.12, Redis requires an additional 'SKIPME no' parameter for killing its own connection
    // This parameter is not (yet) supported by our kill command, so we have to
    // use a different client for killing the desired connection on newer Redis servers
    val killingClient: (RedisClient) => RedisClient = {
      val redisVersion = client.info().futureValue flatMap { info =>
        info.lines.collectFirst {
          case RedisVersion(major, minor, mini) => RedisVersion(major, minor, mini)
        }
      }
      val isNewKillSemantics = redisVersion.exists(_ >= RedisVersion(2, 8, 12))
      if (isNewKillSemantics) {
        val differentClient = RedisClient("localhost", 6379)
        (_: RedisClient) => differentClient
      } else identity
    }

    def killClientsNamed(rc: RedisClient, name: String): Future[List[Boolean]] = {
      // Clients are a list of lines similar to
      // addr=127.0.0.1:65227 fd=9 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client
      // We'll split them up and make a map
      val clients = rc.client.list().futureValue.get.toString
        .split('\n')
        .map(_.trim)
        .filterNot(_.isEmpty)
        .map(
          _.split(" ").map(
            _.split("=").padTo(2, "")
          ).map(
            item => (item(0), item(1))
          )
        ).map(_.toMap)
      val killCandidates = clients.filter(_("name") == name).map(_("addr")).toList
      Future.sequence(killCandidates.map(killingClient(rc).client.kill))
    }

    it("should not reconnect by default") {
      val name = "test-client-1"
      client.client.setname(name).futureValue should equal (true)

      val probe = TestProbe()
      probe watch client.clientRef
      killClientsNamed(client, name).futureValue.reduce(_ && _) should equal (true)
      probe.expectTerminated(client.clientRef)
    }

    it("should reconnect with settings") {
      withReconnectingClient { client =>
        val name = "test-client-2"
        client.client.setname(name).futureValue should equal (true)

        val key = "reconnect_test"
        client.lpush(key, 0)

        killClientsNamed(client, name).futureValue.reduce(_ && _) should equal (true)

        client.lpush(key, 1 to 100).futureValue should equal(101)
        val list = client.lrange[Long](key, 0, -1).futureValue

        list.size should equal(101)
        list.reverse should equal(0 to 100)
      }
    }
  }
}
