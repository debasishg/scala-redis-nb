package com.redis
package serialization


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
  def apply[T](f: String => T) = new Parse[T](f)

  object Implicits {
    implicit val parseString = Parse[String](identity)
    implicit val parseByteArray = Parse[Array[Byte]](_.getBytes("UTF-8"))
    implicit val parseInt = Parse[Int](java.lang.Integer.parseInt)
    implicit val parseShort = Parse[Short](java.lang.Short.parseShort)
    implicit val parseLong = Parse[Long](java.lang.Long.parseLong)
    implicit val parseFloat = Parse[Float](java.lang.Float.parseFloat)
    implicit val parseDouble = Parse[Double](java.lang.Double.parseDouble)
  }

  implicit val parseDefault = Implicits.parseString
}

class Parse[A](val f: String => A) extends Function1[String, A] {
  def apply(in: String): A = f(in)
}
