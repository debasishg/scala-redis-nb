package com.redis.serialization

import scala.concurrent.Future

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.protocol.StringCommands
import com.redis.RedisSpecBase


@RunWith(classOf[JUnitRunner])
class SerializationSpec extends RedisSpecBase {
  import StringCommands._

  describe("serialization") {
    it("should set values to keys") {
      import DefaultFormats._
      client.set("key100", 200).futureValue should be (true)
      client.get[Long]("key100").futureValue should be (Some(200))
    }

    it("should not conflict when using all built in parsers") {
      import DefaultFormats._
      client.hmset("hash", Map("field1" -> "1", "field2" -> 2)).futureValue should be (true)
      client.hmget[String]("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))
      client.hmget[Int]("hash", "field1", "field2").futureValue should be(Map("field1" -> 1, "field2" -> 2))
      client.hmget[Int]("hash", "field1", "field2", "field3").futureValue should be(Map("field1" -> 1, "field2" -> 2))
    }

    it("should use a provided implicit parser") {
      import DefaultFormats._
      client.hmset("hash", Map("field1" -> "1", "field2" -> 2)).futureValue should be (true)

      client.hmget("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))

      implicit val intFormat = Format[Int](java.lang.Integer.parseInt, _.toString)

      client.hmget[Int]("hash", "field1", "field2").futureValue should be(Map("field1" -> 1, "field2" -> 2))
      client.hmget[String]("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "2"))
      client.hmget[Int]("hash", "field1", "field2", "field3").futureValue should be(Map("field1" -> 1, "field2" -> 2))
    }

    it("should use a provided implicit string parser") {
      import DefaultFormats._
      implicit val stringFormat = Format[String](new String(_).toInt.toBinaryString, identity)
      client.hmset("hash", Map("field1" -> "1", "field2" -> 2)).futureValue should be (true)
      client.hmget[Int]("hash", "field1", "field2").futureValue should be(Map("field1" -> 1, "field2" -> 2))
      client.hmget[String]("hash", "field1", "field2").futureValue should be(Map("field1" -> "1", "field2" -> "10"))
    }

    it("should use a provided implicit writer") {
      import DefaultFormats._
      import Stringified._
      import KeyValuePair._

      case class Upper(s: String)

      // using Stringified just uses default toString serialization
      client.hmset("hash1", Map("field1" -> Stringified(Upper("val1")), "field2" -> Stringified(Upper("val2")))).futureValue should be (true)

      // provide an instance of Write typeclass for custom writes 
      implicit val w = Write[Upper]( {case Upper(s) => s.toUpperCase} )

      client.hmset("hash2", Map("field1" -> Upper("val1"), "field2" -> Upper("val2"))).futureValue should be (true)
      client.hmget("hash1", "field1", "field2").futureValue should be(Map("field1" -> "Upper(val1)", "field2" -> "Upper(val2)"))
      client.hmget("hash2", "field1", "field2").futureValue should be(Map("field1" -> "VAL1", "field2" -> "VAL2"))
    }

    it("should parse string as a bytearray with an implicit parser") {
      import DefaultFormats.byteArrayFormat
      val x = "debasish".getBytes("UTF-8")
      client.set("key", x).futureValue should be(true)

      val s = client.get[Array[Byte]]("key").futureValue
      new String(s.get, "UTF-8") should equal("debasish")

      client.get[Array[Byte]]("keey").futureValue should equal(None)
    }
  }
}

