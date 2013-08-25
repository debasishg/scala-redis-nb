package com.redis.serialization


import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith


// should be top-level for equality test
case class Person(id: Int, name: String)

@RunWith(classOf[JUnitRunner])
class SerializationSpec extends FunSpec with Matchers {


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

    val debasish = Person(1, "Debasish Gosh")
    val jisoo = Person(2, "Jisoo Park")
    val people = List(debasish, jisoo)

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
