package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import akka.util.ByteString
import com.redis.protocol.StringCommands
import com.redis.RedisSpecBase
import com.redis.serialization.Stringified


@RunWith(classOf[JUnitRunner])
class StringOperationsSpec extends RedisSpecBase {
  import StringCommands._

  describe("set") {
    it("should set values to keys") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("key" + num, "value" + num) }).unzip
      val writes = keys zip values map { case (key, value) => client.set(key, value) }

      Future.sequence(writes).futureValue should contain only (true)
    }

    it("should not set values to keys already existing with option NX") {
      val key = "key100"
      client.set(key, "value100").futureValue should be (true)

      client
        .set(key, "value200", None, Some(NX))
        .futureValue match {
          case true => fail("an existing key with an value should not be set with NX option")
          case false => client.get(key).futureValue should equal (Some("value100"))
        }

      // convenient alternative
      client
        .set(key, "value200", NX)
        .futureValue match {
        case true => fail("an existing key with an value should not be set with NX option")
        case false => client.get(key).futureValue should equal (Some("value100"))
      }
    }

    it("should not set values to non-existing keys with option XX") {
      val key = "value200"
      client
        .set(key, "value200", None, Some(XX))
        .futureValue match {
          case true => fail("set on a non existing key with XX will fail")
          case false => client.get(key).futureValue should equal (None)
        }

      // convenient alternative
      client
        .set(key, "value200", XX)
        .futureValue match {
          case true => fail("set on a non existing key with XX will fail")
          case false => client.get(key).futureValue should equal (None)
        }
    }

    it("should not encode a byte array to a UTF-8 string") {
      val bytes = Array(0x85.toByte)

      client.set("bytes", bytes)
      client.get[String]("bytes").futureValue.get should not equal (bytes)
      client.get[Array[Byte]]("bytes").futureValue.get.toList should equal (bytes.toList)
    }
  }

  describe("get") {
    it("should get results for keys set earlier") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("get" + num, "value" + num) }).unzip
      keys zip values foreach { case (key, value) => client.set(key, value) }

      val reads = keys map (client.get(_))
      val readResults = Future.sequence(reads).futureValue

      readResults zip values foreach { case (result, expectedValue) =>
        result should equal (Some(expectedValue))
      }
      readResults should equal (List(Some("value1"), Some("value2"), Some("value3")))
    }

    it("should give none for unknown keys") {
      client.get("get_unkown").futureValue should equal (None)
    }
  }

  describe("mset") {
    it("should set multiple keys") {
      val numKeys = 3
      val keyvals = (1 to numKeys map { num => ("get" + num, "value" + num) })
      client.mset(keyvals: _*).futureValue should be (true)

      val (keys, vals) = keyvals.unzip
      val reads = keys map (client.get(_))
      val readResults = Future.sequence(reads).futureValue
      readResults should equal (vals.map(Some.apply))
    }

    it("should support various value types in a single command") {
      import com.redis.serialization.DefaultFormats._

      client
        .mset(("int" -> 1), ("long" -> 2L), ("pi" -> 3.14), ("string" -> "string"))
        .futureValue should be (true)

      client
        .mget("int", "long", "pi", "string")
        .futureValue should equal (Map("int" -> "1", "long" -> "2", "pi" -> "3.14", "string" -> "string"))
    }
  }

  describe("mget") {
    it("should get results from existing keys") {
      val numKeys = 3
      val keyvals = (1 to numKeys map { num => ("get" + num, "value" + num) })
      client.mset(keyvals: _*).futureValue

      val (keys, _) = keyvals.unzip
      val readResult = client.mget("nonExistingKey", keys.take(2): _*).futureValue
      readResult should equal (keyvals.take(2).toMap)
    }
  }

  describe("bitop") {
    it("should perform bitwise operations") {
      val _ = Future.sequence(
        client.set("key1", "abc") ::
        client.set("key2", "def") ::
        Nil
      ).futureValue

      val res = (for {
        _ <- client.bitop("AND", "dest", "key1", "key2")
        r <- client.get("dest")
      } yield r).futureValue

      res should equal (Some("``b"))
    }

    // Refer to https://github.com/debasishg/scala-redis-nb/issues/27
    it("should handle irregular byte arrays") {
      val _ = Future.sequence(
          client.set("key", "z") ::
          Nil
      ).futureValue

      val res = (for {
        _ <- client.bitop("NOT", "dest", "key")
        r <- client.get[Array[Byte]]("dest")
      } yield r).futureValue

      res.get should equal (Array(0x85.toByte))
    }
  }

  describe("bitpos") {
    it("should return the position of the first bit set to 1") {
      val key = "bitpos1"
      val bits = Stringified(ByteString(0x00, 0xf0, 0x00).map(_.toByte))
      client.set(key, bits).futureValue should equal (true)
      val bit = true
      client.bitpos(key, bit).futureValue should equal (8)
    }

    it("should return the position of the first bit set to 0") {
      val key = "bitpos2"
      val bits = Stringified(ByteString(Array(0xff, 0xf0, 0x00).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      client.bitpos(key, bit).futureValue should equal (12)
    }

    it("should return the position of hte first bit set to 1 after start position") {
      val key = "bitpos3"
      val bits = Stringified(ByteString(Array(0xff, 0xf0, 0xf0, 0xf0, 0x00).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      client.bitpos(key, bit, 2).futureValue should equal (20)
    }

    it("should return the position of the first bit set to 1 between start and end") {
      val key = "bitpos4"
      val bits = Stringified(ByteString(Array(0xff, 0xf0, 0xf0, 0xf0, 0x00).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      client.bitpos(key, bit, 2, 3).futureValue should equal (20)
    }

    it("should return -1 if 1 bit is specified and the string is composed of just zero bytes") {
      val key = "bitpos5"
      val bits = Stringified(ByteString(Array(0x00, 0x00, 0x00).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = true
      client.bitpos(key, bit).futureValue should equal (-1)
    }

    it("should return the first bit not part of the string on the right if 0 bit is specified and the string only contains bit set to 1") {
      val key = "bitpos6"
      val bits = Stringified(ByteString(Array(0xff, 0xff, 0xff).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      client.bitpos(key, bit).futureValue should equal (24)
    }

    it("should return -1 if 0 bit is specified and the string contains and clear bit is not found in the specified range") {
      val key = "bitpos7"
      val bits = Stringified(ByteString(Array(0xff, 0xff, 0xff).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      client.bitpos(key, bit, 0, 2).futureValue should equal (-1)
    }

    it("should fail if start is not specified but end is specified") {
      val key = "bitpos8"
      val bits = Stringified(ByteString(Array(0xff, 0xff, 0xff).map(_.toByte)))
      client.set(key, bits).futureValue should equal (true)
      val bit = false
      intercept[IllegalArgumentException] {
        client.bitpos(key, bit, None, Some(2)).futureValue
      }
    }
  }

}
