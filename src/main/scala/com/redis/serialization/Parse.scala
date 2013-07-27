package com.redis.serialization


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
