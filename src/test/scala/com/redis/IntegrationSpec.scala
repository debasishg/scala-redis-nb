package com.redis

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import scala.concurrent.Future
import com.redis.serialization._


object IntegrationSpec {
  // should not be path dependent for equality test
  case class Person(id: Int, name: String)
}

@RunWith(classOf[JUnitRunner])
class IntegrationSpec extends RedisSpecBase {
  import IntegrationSpec._


  describe("DefaultFormats integration") {
    import com.redis.serialization.DefaultFormats._

    it("should set values to keys") {
      client.set("key100", 200).futureValue should be (true)
      client.get[Long]("key100").futureValue should be (Some(200))
    }

    it("should not conflict when using all built in parsers") {
      client
        .hmset("hash", Map("field1" -> "1", "field2" -> 2))
        .futureValue should be (true)

      client
        .hmget[String]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> "1", "field2" -> "2"))

      client
        .hmget[Int]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> 1, "field2" -> 2))

      client
        .hmget[Int]("hash", "field1", "field2", "field3")
        .futureValue should equal (Map("field1" -> 1, "field2" -> 2))
    }

    it("should use a provided implicit parser") {
      client
        .hmset("hash", Map("field1" -> "1", "field2" -> 2))
        .futureValue should be (true)

      client
        .hmget("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> "1", "field2" -> "2"))

      implicit val intFormat = Format[Int](java.lang.Integer.parseInt, _.toString)

      client
        .hmget[Int]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> 1, "field2" -> 2))

      client
        .hmget[String]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> "1", "field2" -> "2"))

      client
        .hmget[Int]("hash", "field1", "field2", "field3")
        .futureValue should equal (Map("field1" -> 1, "field2" -> 2))
    }

    it("should use a provided implicit string parser") {
      implicit val stringFormat = Format[String](new String(_).toInt.toBinaryString, identity)

      client
        .hmset("hash", Map("field1" -> "1", "field2" -> 2))
        .futureValue should be (true)

      client
        .hmget[Int]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> 1, "field2" -> 2))

      client
        .hmget[String]("hash", "field1", "field2")
        .futureValue should equal (Map("field1" -> "1", "field2" -> "10"))
    }

    it("should parse string as a bytearray with an implicit parser") {
      val x = "debasish".getBytes("UTF-8")
      client.set("key", x).futureValue should be (true)

      val s = client.get[Array[Byte]]("key").futureValue
      new String(s.get, "UTF-8") should equal ("debasish")

      client.get[Array[Byte]]("keey").futureValue should equal (None)
    }
  }

  describe("Custom format support") {

    val debasish = Person(1, "Debasish Gosh")

    it("should be easy to customize") {

      implicit val customPersonFormat =
        new Format[Person] {
          def read(str: String) = {
            val head :: rest = str.split('|').toList
            val id = head.toInt
            val name = rest.mkString("|")

            Person(id, name)
          }

          def write(person: Person) = {
            import person._
            s"$id|$name"
          }
        }

      val _ = client.set("debasish", debasish).futureValue

      client.get[Person]("debasish").futureValue should equal (Some(debasish))
    }
  }

  describe("Third-party library integration") {

    val debasish = Person(1, "Debasish Gosh")
    val jisoo = Person(2, "Jisoo Park")

    it("should support out-of-box (un)marshalling") {
      import spray.json.DefaultJsonProtocol._
      import SprayJsonSupport._

      implicit val personFormat = jsonFormat2(Person)

      val _ = Future.sequence(
        client.set("debasish", debasish) ::
        client.set("people", List(debasish, jisoo)) ::
        Nil
      ).futureValue

      client.get[Person]("debasish").futureValue should equal (Some(debasish))
      client.get[List[Person]]("people").futureValue should equal (Some(List(debasish, jisoo)))
    }

  }

}
