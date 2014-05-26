package com.redis.serialization

import scala.language.implicitConversions


trait SprayJsonSupport {
  import spray.json._

  implicit def sprayJsonStringReader[A](implicit reader: RootJsonReader[A]): Reader[A] =
    StringReader(s => reader.read(s.parseJson))

  implicit def sprayJsonStringWriter[A](implicit writer: RootJsonWriter[A]): Writer[A] =
    StringWriter(writer.write(_).toString)
}

object SprayJsonSupport extends SprayJsonSupport


trait Json4sSupport {
  import org.json4s.{Serialization, Formats}

  def Serialization: Serialization

  implicit def json4sStringReader[A](implicit format: Formats, manifest: Manifest[A]): Reader[A] =
    StringReader(Serialization.read(_))

  implicit def json4sStringWriter[A <: AnyRef](implicit format: Formats): Writer[A] =
    StringWriter(Serialization.write(_))
}

trait Json4sNativeSupport extends Json4sSupport {
  val Serialization = org.json4s.native.Serialization
}

object Json4sNativeSupport extends Json4sNativeSupport

trait Json4sJacksonSupport extends Json4sSupport {
  val Serialization = org.json4s.jackson.Serialization
}

object Json4sJacksonSupport extends Json4sJacksonSupport


//trait LiftJsonSupport {
//  import net.liftweb.json._

//  implicit def liftJsonStringReader[A](implicit format: Formats, manifest: Manifest[A]): Reader[A] =
//    StringReader(parse(_).extract[A])

//  implicit def liftJsonStringWriter[A <: AnyRef](implicit format: Formats): Writer[A] =
//    StringWriter(Serialization.write(_))
//}

//object LiftJsonSupport extends LiftJsonSupport
