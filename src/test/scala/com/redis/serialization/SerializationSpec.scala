package com.redis.serialization


import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis.RedisSpecBase


// should be top-level for equality test
case class Person(id: Int, name: String)

@RunWith(classOf[JUnitRunner])
class SerializationSpec extends RedisSpecBase {

  val debasish = Person(1, "Debasish Gosh")
  val jisoo = Person(2, "Jisoo Park")
  val people = List(debasish, jisoo)

  describe("Serialization") {
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

    it("should parse string as a bytearray with an implicit parser") {
      import DefaultFormats.byteArrayFormat
      val x = "debasish".getBytes("UTF-8")
      client.set("key", x).futureValue should be(true)

      val s = client.get[Array[Byte]]("key").futureValue
      new String(s.get, "UTF-8") should equal("debasish")

      client.get[Array[Byte]]("keey").futureValue should equal(None)
    }
  }

  describe("Format") {
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

      val write = implicitly[Write[Person]].write _
      val read = implicitly[Read[Person]].read _

      read(write(debasish)) should equal (debasish)
    }
  }


  describe("Stringified") {

    it("should stringify String by default") {
      implicitly[String => Stringified].apply("string") should equal (Stringified("string"))
    }

    it("should not conflict with other implicit formats") {
      import DefaultFormats._
      implicitly[String => Stringified].apply("string") should equal (Stringified("string"))
    }

    it("should prioritize closer implicits") {
      @volatile var localWriterUsed = false

      implicit val localWriter: Write[String] = Write { string =>
        localWriterUsed = true
        string
      }

      implicitly[String => Stringified].apply("string")
      localWriterUsed should be (true)
    }
  }


  describe("Integration") {

    describe("SprayJsonSupport") {
      it("should encode/decode json objects") {
        import spray.json.DefaultJsonProtocol._
        import SprayJsonSupport._

        implicit val personFormat = jsonFormat2(Person)

        val write = implicitly[Write[Person]].write _
        val read = implicitly[Read[Person]].read _

        val writeL = implicitly[Write[List[Person]]].write _
        val readL = implicitly[Read[List[Person]]].read _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("Json4sNativeSupport") {
      it("should encode/decode json objects") {
        import Json4sNativeSupport._

        implicit val format = org.json4s.DefaultFormats

        val write = implicitly[Write[Person]].write _
        val read = implicitly[Read[Person]].read _

        val writeL = implicitly[Write[List[Person]]].write _
        val readL = implicitly[Read[List[Person]]].read _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("Json4sJacksonSupport") {
      it("should encode/decode json objects") {
        import Json4sJacksonSupport._

        implicit val format = org.json4s.DefaultFormats

        val write = implicitly[Write[Person]].write _
        val read = implicitly[Read[Person]].read _

        val writeL = implicitly[Write[List[Person]]].write _
        val readL = implicitly[Read[List[Person]]].read _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("LiftJsonSupport") {
      it("should encode/decode json objects") {
        import LiftJsonSupport._

        implicit val format = net.liftweb.json.DefaultFormats

        val write = implicitly[Write[Person]].write _
        val read = implicitly[Read[Person]].read _

        val writeL = implicitly[Write[List[Person]]].write _
        val readL = implicitly[Read[List[Person]]].read _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }
  }


}
