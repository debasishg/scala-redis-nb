package com.redis.serialization

import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


object SerializationSpec {
  // should not be path dependent for equality test
  case class Person(id: Int, name: String)
}

@RunWith(classOf[JUnitRunner])
class SerializationSpec extends FunSpec with Matchers  {
  import SerializationSpec._

  val debasish = Person(1, "Debasish Gosh")
  val jisoo = Person(2, "Jisoo Park")
  val people = List(debasish, jisoo)

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

      val write = implicitly[Writer[Person]].toByteString _
      val read = implicitly[Reader[Person]].fromByteString _

      read(write(debasish)) should equal (debasish)
    }
  }


  describe("Stringified") {

    it("should serialize String by default") {
      implicitly[String => Stringified].apply("string") should equal (Stringified("string"))
    }

    it("should not conflict with other implicit formats") {
      import DefaultFormats._
      implicitly[String => Stringified].apply("string") should equal (Stringified("string"))
    }

    it("should prioritize closer implicits") {
      @volatile var localWriterUsed = false

      implicit val localWriter: Writer[String] = StringWriter { string =>
        localWriterUsed = true
        string
      }

      implicitly[String => Stringified].apply("string")
      localWriterUsed should be (true)
    }

    it("should not encode a byte array to a UTF-8 string") {
      val nonUtf8Bytes = Array(0x85.toByte)

      new String(nonUtf8Bytes, "UTF-8").getBytes("UTF-8") should not equal (nonUtf8Bytes)

      Stringified(nonUtf8Bytes).value.toArray[Byte] should equal (nonUtf8Bytes)
    }
  }


  describe("Integration") {

    describe("SprayJsonSupport") {
      it("should encode/decode json objects") {
        import spray.json.DefaultJsonProtocol._
        import SprayJsonSupport._

        implicit val personFormat = jsonFormat2(Person)

        val write = implicitly[Writer[Person]].toByteString _
        val read = implicitly[Reader[Person]].fromByteString _

        val writeL = implicitly[Writer[List[Person]]].toByteString _
        val readL = implicitly[Reader[List[Person]]].fromByteString _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("Json4sNativeSupport") {
      it("should encode/decode json objects") {
        import Json4sNativeSupport._

        implicit val format = org.json4s.DefaultFormats

        val write = implicitly[Writer[Person]].toByteString _
        val read = implicitly[Reader[Person]].fromByteString _

        val writeL = implicitly[Writer[List[Person]]].toByteString _
        val readL = implicitly[Reader[List[Person]]].fromByteString _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("Json4sJacksonSupport") {
      it("should encode/decode json objects") {
        import Json4sJacksonSupport._

        implicit val format = org.json4s.DefaultFormats

        val write = implicitly[Writer[Person]].toByteString _
        val read = implicitly[Reader[Person]].fromByteString _

        val writeL = implicitly[Writer[List[Person]]].toByteString _
        val readL = implicitly[Reader[List[Person]]].fromByteString _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }

    describe("LiftJsonSupport") {
      it("should encode/decode json objects") {
        import LiftJsonSupport._

        implicit val format = net.liftweb.json.DefaultFormats

        val write = implicitly[Writer[Person]].toByteString _
        val read = implicitly[Reader[Person]].fromByteString _

        val writeL = implicitly[Writer[List[Person]]].toByteString _
        val readL = implicitly[Reader[List[Person]]].fromByteString _

        read(write(debasish)) should equal (debasish)
        readL(writeL(people)) should equal (people)
      }
    }
  }


}
