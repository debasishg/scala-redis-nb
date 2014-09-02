package com.redis.api

import akka.util.ByteString
import com.redis.RedisConnection.CommandRejected
import com.redis.{RedisClient, RedisSpecBase}

import scala.concurrent.Await

class PubSubOperationsSpec extends RedisSpecBase {
  import com.redis.protocol.PubSubCommands._

import scala.concurrent.duration._

  def withClientPerTest( testCode : RedisClient => Any ): Unit = {
    val redisClient = RedisClient("localhost", 6379)
    try { testCode(redisClient) } finally redisClient.quit()
  }

  def sendReceive(channel: String, message: String): Unit = {
    client.publish(channel, message).futureValue should equal(1)
    val msg = expectMsgType[Message]
    msg.payload should equal( ByteString(message) )
  }

  def psendReceive(channel: String, message: String): Unit = {
    client.publish(channel, message).futureValue should equal(1)
    val msg = expectMsgType[PMessage]
    msg.payload should equal( ByteString(message) )
  }

  describe("Pub/Sub") {

    it("should subscribe to some channels and deliver subscription notifications") {
      withClientPerTest { subClient =>
        subClient.subscribe(testActor, "first", "second")
        expectMsgAllOf( Subscribed("first", false, 1), Subscribed("second", false, 2) )
      }
    }

    it("accepts only QUIT, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE and PUNSUBSCRIBE  while in the subscribed state") {
      withClientPerTest { subClient =>
        subClient.subscribe( testActor, "first" )
        expectMsgType[Subscribed]
        val future = subClient.set("key", "value")
        Await.result( future.failed, 1.second ) shouldBe a [CommandRejected]
      }
    }

    it("subscription can be canceled") {
      withClientPerTest { subClient =>
        subClient.subscribe(testActor, "first", "second")
        expectMsgType[Subscribed]
        expectMsgType[Subscribed]

        sendReceive( "first", "1" )

        subClient.unsubscribe("second")
        expectMsgType[Unsubscribed]
        sendReceive("first", "2")
        client.publish("second", "3").futureValue should equal(0)
        expectNoMsg()
      }
    }

    it("Should be able publish messages") {
      withClientPerTest { subClient =>
        subClient.clientRef tell (Subscribe("first"), testActor)
        expectMsg( Subscribed("first", false, 1) )

        sendReceive("first", "something")
      }
    }

    it("allows to subscribe to the further channels") {
      withClientPerTest { subClient =>
        subClient.subscribe( testActor, "1")
        expectMsgType[Subscribed]
        client.publish("2", "msg").futureValue should equal(0)
        expectNoMsg(1.second)
        subClient.subscribe( testActor, "2" )
        expectMsgType[Subscribed]
        sendReceive("2", "msg")
      }
    }

    it("allows to subscribe and unsubscribe using patterns") {
      withClientPerTest { subClient =>
        val pattern = "h?llo"
        subClient.psubscribe( testActor, pattern)
        expectMsgType[Subscribed]

        psendReceive("hello", "hellomsg")
        psendReceive("hallo", "hallomsg")

        subClient.punsubscribe( "hell?" )
        client.publish("hillo", "hellomsg")
        client.publish("hallo", "hallomsg")
        expectMsgType[Unsubscribed]
        // still receive messages
        expectMsgType[PMessage]
        expectMsgType[PMessage]

        subClient.punsubscribe(pattern)
        expectMsgType[Unsubscribed]
        client.publish("hillo", "hellomsg")
        client.publish("hallo", "hallomsg")
        expectNoMsg()

      }
    }
  }

}
