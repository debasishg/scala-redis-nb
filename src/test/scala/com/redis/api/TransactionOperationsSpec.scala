package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import com.redis.RedisSpecBase
import com.redis.protocol.Discarded
import akka.actor.Status.Failure
import com.redis.serialization.Deserializer._


@RunWith(classOf[JUnitRunner])
class TransactionOperationsSpec extends RedisSpecBase {

  import com.redis.serialization.DefaultFormats._

  describe("transactions with API") {
    it("should use API") {
      val result = client.withTransaction {c =>
        c.set("anshin-1", "debasish") 
        c.exists("anshin-1")
        c.get("anshin-1")
        c.set("anshin-2", "debasish")
        c.lpush("anshin-3", "debasish") 
        c.lpush("anshin-3", "maulindu") 
        c.lrange("anshin-3", 0, -1)
      }
      result.futureValue should equal (List(true, true, Some("debasish"), true, 1, 2, List("maulindu", "debasish")))
      client.get("anshin-1").futureValue should equal(Some("debasish"))
    }

    it("should execute partial set and report failure on exec") {
      val result = client.withTransaction {c =>
        c.set("anshin-1", "debasish") 
        c.exists("anshin-1")
        c.get("anshin-1")
        c.set("anshin-2", "debasish")
        c.lpush("anshin-2", "debasish") 
        c.lpush("anshin-3", "maulindu") 
        c.lrange("anshin-3", 0, -1)
      }
      val r = result.futureValue.asInstanceOf[List[_]].toVector
      r(0) should equal(true)
      r(1) should equal(true)
      r(2) should equal(Some("debasish"))
      r(3) should equal(true)
      r(4).asInstanceOf[akka.actor.Status.Failure].cause.isInstanceOf[Throwable] should equal(true)
      r(5) should equal(1)
      r(6) should equal(List("maulindu"))
      client.get("anshin-1").futureValue should equal(Some("debasish"))
    }

    it("should convert individual commands using serializers") {
      val result = client.withTransaction {c =>
        c.set("anshin-1", 10) 
        c.exists("anshin-1")
        c.get[Long]("anshin-1")
        c.set("anshin-2", "debasish")
        c.lpush("anshin-3", 200) 
        c.lpush("anshin-3", 100) 
        c.lrange[Long]("anshin-3", 0, -1)
      }
      result.futureValue should equal (List(true, true, Some(10), true, 1, 2, List(100, 200)))
    }

    it("should not convert a byte array to an UTF-8 string") {
      val bytes = Array(0x85.toByte)
      val result = client.withTransaction {c =>
        c.set("anshin-1", 10) 
        c.exists("anshin-1")
        c.get[Long]("anshin-1")
        c.set("bytes", bytes)
        c.get[Array[Byte]]("bytes")
      }
      result.futureValue
            .asInstanceOf[List[Any]]
            .last
            .asInstanceOf[Option[Array[Byte]]].get.toList should equal (bytes.toList) 
    }

    it("should discard the txn queue since user tries to get the future value without exec") {
      val result = client.withTransaction {c =>
        c.set("anshin-1", "debasish").futureValue 
        c.exists("anshin-1")
        c.get("anshin-1")
        c.set("anshin-2", "debasish")
        c.lpush("anshin-3", "debasish") 
        c.lpush("anshin-3", "maulindu") 
        c.lrange("anshin-3", 0, -1)
      }
      result.futureValue should equal (Discarded)
      client.get("anshin-1").futureValue should equal(None)
    }

    it("should execute a mix of transactional and non transactional operations") {
      client.set("k1", "v1").futureValue
      client.get("k1").futureValue should equal(Some("v1"))
      val result = client.withTransaction {c =>
        c.set("nri-1", "debasish") 
        c.exists("nri-1")
        c.get("nri-1")
        c.set("nri-2", "debasish")
        c.lpush("nri-3", "debasish")
        c.lpush("nri-3", "maulindu")
        c.lrange("nri-3", 0, -1)
      }
      result.futureValue should equal (List(true, true, Some("debasish"), true, 1, 2, List("maulindu", "debasish")))
    }

    it("should execute transactional operations along with business logic") {
      val l = List(1,2,3,4) // just to have some business logic
      val result = client.withTransaction {c =>
        c.set("nri-1", "debasish") 
        c.set("nri-2", "maulindu") 
        val x = if (l.size == 4) c.get("nri-1") else c.get("nri-2")
        c.lpush("nri-3", "debasish")
        c.lpush("nri-3", "maulindu")
        c.lrange("nri-3", 0, -1)
      }
      result.futureValue should equal (List(true, true, Some("debasish"), 1, 2, List("maulindu", "debasish")))
    }
  }

  describe("Transactions with watch") {
    it("should fail if key is changed outside transaction") {
      client.watch("key1").futureValue should equal(true)
      client.set("key1", 1).futureValue should equal(true)
      val v = client.get[Long]("key1").futureValue.get + 20L
      val result = client.withTransaction {c =>
        c.set("key1", 100)
        c.get[Long]("key1")
      }
      result onComplete {
        case util.Success(_) => 
        case util.Failure(th) => th should equal(EmptyTxnResultException)
      }
    }

    it("should set key value") {
      client.watch("key1").futureValue should equal(true)
      val result = client.withTransaction {c =>
        c.set("key1", 100)
        c.get[Long]("key1")
      }
      result.futureValue should equal(List(true, Some(100)))
    }

    it("should not fail if key is unwatched") {
      client.watch("key1").futureValue should equal(true)
      client.set("key1", 1).futureValue should equal(true)
      val result = client.withTransaction {c =>
        c.set("key1", 100)
        c.get[Long]("key1")
      }
      result onComplete {
        case util.Success(_) => 
        case util.Failure(th) => th should equal(EmptyTxnResultException)
      }
      client.unwatch().futureValue should equal(true)
      client.set("key1", 5).futureValue should equal(true)
      val res = client.withTransaction {c =>
        c.set("key1", 100)
        c.get[Long]("key1")
      }
      res.futureValue should equal(List(true, Some(100)))
    }
  }

}
