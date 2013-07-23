package com.redis

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import serialization._


@RunWith(classOf[JUnitRunner])
class StringOperationsSpec extends RedisSpecBase {
  import RedisCommand._
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

      val v = client.set(key, "value200", Some(NX))
      v.futureValue match {
        case true => fail("an existing key with an value should not be set with NX option")
        case false => client.get(key).futureValue should equal (Some("value100"))
      }
    }

    it("should not set values to non-existing keys with option XX") {
      val key = "value200"
      val v = client.set(key, "value200", Some(XX))
      v.futureValue match {
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

      val reads = keys map { key => client.get(key) }
      val readResults = Future.sequence(reads).futureValue

      readResults zip values foreach { case (result, expectedValue) =>
        result should equal (Some(expectedValue))
      }
      readResults should equal (List(Some("value1"), Some("value2"), Some("value3")))
    }

    it("should give none for unknown keys") {
      val reads = client.get("get_unkown")
      reads.futureValue should equal (None)
    }
  }

}
