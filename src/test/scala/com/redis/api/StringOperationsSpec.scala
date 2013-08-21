package com.redis.api

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.protocol.StringCommands
import com.redis.RedisSpecBase


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
  }

  describe("get") {
    it("should get results for keys set earlier") {
      val numKeys = 3
      val (keys, values) = (1 to numKeys map { num => ("get" + num, "value" + num) }).unzip
      val writes = keys zip values map { case (key, value) => client.set(key, value) }
      val writeResults = Future.sequence(writes).futureValue

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

}
