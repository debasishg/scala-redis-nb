package com.redis.serialization

import scala.annotation.implicitNotFound
import scala.language.implicitConversions


@implicitNotFound(msg = "Cannot find implicit Read or Format type class for ${A}")
trait Read[A] {
  val forByteArray: Boolean = false

  def read(in: String): A
}

trait LowPriorityDefaultReader {
  implicit object byteArrayReader extends Read[Array[Byte]] {
    override val forByteArray = true

    def read(in: String): Array[Byte] = throw new Error("Not intended to use directly")
  }
}

object Read extends LowPriorityDefaultReader {
  def apply[A](f: String => A) = new Read[A] { def read(in: String) = f(in) }

  implicit def default: Read[String] = DefaultFormats.stringFormat
}


@implicitNotFound(msg = "Cannot find implicit Write or Format type class for ${A}")
trait Write[A] {
  def write(in: A): String
}

object Write {
  def apply[A](f: A => String) = new Write[A] { def write(in: A) = f(in) }

  implicit def default: Write[String] = DefaultFormats.stringFormat

  private[redis] object Internal {
    def formatBoolean(b: Boolean) = if (b) "1" else "0"

    def formatDouble(d: Double, inclusive: Boolean = true) =
      (if (inclusive) ("") else ("(")) + {
        if (d.isInfinity) {
          if (d > 0.0) "+inf" else "-inf"
        } else {
          d.toString
        }
      }
  }
}


trait Format[A] extends Read[A] with Write[A]

object Format {

  def apply[A](_read: String => A, _write: A => String) = new Format[A] {
    def read(str: String) = _read(str)

    def write(obj: A) = _write(obj)
  }

  implicit def default = DefaultFormats.stringFormat
}

private[serialization] trait LowPriorityDefaultFormats {
  implicit val byteArrayFormat = Format[Array[Byte]](_.getBytes("UTF-8"), new String(_))
}

private[serialization] trait LowPriorityFormats extends LowPriorityDefaultFormats {
  import java.{lang => J}
  implicit val intFormat = Format[Int](J.Integer.parseInt, _.toString)
  implicit val shortFormat = Format[Short](J.Short.parseShort, _.toString)
  implicit val longFormat = Format[Long](J.Long.parseLong, _.toString)
  implicit val floatFormat = Format[Float](J.Float.parseFloat, _.toString)
  implicit val doubleFormat = Format[Double](J.Double.parseDouble, _.toString)
  implicit val anyFormat = Format[Any](identity, _.toString)
}

trait DefaultFormats extends LowPriorityFormats {
  implicit val stringFormat = Format[String](identity, identity)
}

object DefaultFormats extends DefaultFormats
