package com.redis.serialization


object Write {
  def apply[A](f: A => String) = new Write[A](f)

  object Implicits {
    implicit val writeString = Write[String](identity)
    implicit val writeByteArray = Write[Array[Byte]](new String(_))
    implicit val writeInt = Write[Int](_.toString)
    implicit val writeShort = Write[Short](_.toString)
    implicit val writeLong = Write[Long](_.toString)
    implicit val writeFloat = Write[Float](_.toString)
    implicit val writeDouble = Write[Double](_.toString)
  }

  implicit def default = Implicits.writeString

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

class Write[A](val f: A => String) extends (A => String) {
  def apply(in: A): String = f(in)
}

