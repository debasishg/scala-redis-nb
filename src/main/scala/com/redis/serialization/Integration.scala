package com.redis.serialization

import scala.language.implicitConversions


trait SprayJsonSupport {
  import spray.json._

  implicit def sprayJsonReader[A](implicit reader: RootJsonReader[A]): Reader[A] =
    Reader(s => reader.read(s.asJson))

  implicit def sprayJsonWriter[A](implicit writer: RootJsonWriter[A]): Writer[A] =
    Writer(writer.write(_).toString)
}

object SprayJsonSupport extends SprayJsonSupport


trait Json4sSupport {
  import org.json4s.{Serialization, Formats}

  def Serialization: Serialization

  implicit def json4sReader[A](implicit format: Formats, manifest: Manifest[A]): Reader[A] =
    Reader(Serialization.read(_))

  implicit def json4sWriter[A <: AnyRef](implicit format: Formats): Writer[A] =
    Writer(Serialization.write(_))
}

trait Json4sNativeSupport extends Json4sSupport {
  val Serialization = org.json4s.native.Serialization
}

object Json4sNativeSupport extends Json4sNativeSupport

trait Json4sJacksonSupport extends Json4sSupport {
  val Serialization = org.json4s.jackson.Serialization
}

object Json4sJacksonSupport extends Json4sJacksonSupport


trait LiftJsonSupport {
  import net.liftweb.json._

  implicit def liftJsonReader[A](implicit format: Formats, manifest: Manifest[A]): Reader[A] =
    Reader(parse(_).extract[A])

  implicit def liftJsonWriter[A <: AnyRef](implicit format: Formats): Writer[A] =
    Writer(Serialization.write(_))
}

object LiftJsonSupport extends LiftJsonSupport
