package com.redis.serialization

import scala.annotation.implicitNotFound
import scala.language.implicitConversions


@implicitNotFound(msg = "Cannot find implicit Read or Format type class for ${A}")
private[redis] trait Reader[A] {
  def fromByteString(in: ByteString): A
}

trait StringReader[A] extends Reader[A] {
  def read(in: String): A

  def fromByteString(in: ByteString): A = read(in.utf8String)
}

private[redis] trait LowPriorityDefaultReader {
  implicit object bypassingReader extends Reader[ByteString] {
    def fromByteString(in: ByteString) = in
  }

  implicit object byteArrayReader extends Reader[Array[Byte]] {
    def fromByteString(in: ByteString) = in.toArray[Byte]
  }
}

object Reader extends LowPriorityDefaultReader {
  def apply[A](f: String => A) = new StringReader[A] { def read(in: String) = f(in) }

  implicit def default: Reader[String] = DefaultFormats.stringFormat
}


@implicitNotFound(msg = "Cannot find implicit Write or Format type class for ${A}")
private[redis] trait Writer[A] {
  private[redis] def toByteString(in: A): ByteString
}

trait StringWriter[A] extends Writer[A] {
  def write(in: A): String

  def toByteString(in: A): ByteString = ByteString(write(in))
}


object Writer {
  def apply[A](f: A => String) = new StringWriter[A] { def write(in: A) = f(in) }

  implicit def default: Writer[String] = DefaultFormats.stringFormat

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



trait Format[A] extends StringReader[A] with StringWriter[A]

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
