package com.redis.serialization


object Read {
  def apply[T](f: String => T) = new Read[T](f)

  object Implicits {
    implicit val readString = Read[String](identity)
    implicit val readByteArray = Read[Array[Byte]](_.getBytes("UTF-8"))
    implicit val readInt = Read[Int](java.lang.Integer.parseInt)
    implicit val readShort = Read[Short](java.lang.Short.parseShort)
    implicit val readLong = Read[Long](java.lang.Long.parseLong)
    implicit val readFloat = Read[Float](java.lang.Float.parseFloat)
    implicit val readDouble = Read[Double](java.lang.Double.parseDouble)
  }

  implicit def parseDefault = Implicits.readString
}

class Read[A](val f: String => A) extends (String => A) {
  def apply(in: String): A = f(in)
}
