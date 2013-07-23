package com.redis
package serialization

import akka.util.ByteString


object Format {
  def apply(f: PartialFunction[Any, Any]): Format = new Format(f)

  implicit val default: Format = new Format(Map.empty)

  def formatDouble(d: Double, inclusive: Boolean = true) =
    (if (inclusive) ("") else ("(")) + {
      if (d.isInfinity) {
        if (d > 0.0) "+inf" else "-inf"
      } else {
        d.toString
      }
    }
}

class Format(val format: PartialFunction[Any, Any]) {
  def apply(in: Any): String =
    (if (format.isDefinedAt(in)) (format(in)) else (in)) match {
      case d: Double => Format.formatDouble(d, true)
      case x => x.toString
    }

  def orElse(that: Format): Format = Format(format orElse that.format)

  def orElse(that: PartialFunction[Any, Any]): Format = Format(format orElse that)
}

object Parse {
  def apply[T](f: (ByteString) => T) = new Parse[T](f)

  object Implicits {
    implicit val parseString = Parse[String](_.utf8String)
    implicit val parseByteArray = Parse[Array[Byte]](_.toArray[Byte])
    implicit val parseInt = Parse[Int](_.utf8String.toInt)
    implicit val parseLong = Parse[Long](_.utf8String.toLong)
    implicit val parseDouble = Parse[Double](_.utf8String.toDouble)
  }

  implicit val parseDefault = Parse[String](_.utf8String)
}

class Parse[A](val f: (ByteString) => A) extends Function1[ByteString, A] {
  def apply(in: ByteString): A = f(in)
}
