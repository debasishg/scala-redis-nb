package com.redis.serialization

import com.redis.protocol._
import ByteStringReader._


trait PartialDeserializer[A] extends PartialFunction[RawReply, A]

class PrefixDeserializer[A](prefix: Byte, f: RawReply => A) extends PartialDeserializer[A] {
  def isDefinedAt(x: RawReply) = x.head == prefix
  def apply(r: RawReply) = {
    r.jump(1)
    f(r)
  }
}

object PartialDeserializer extends LowPriorityPD {

  def apply[A](pf: PartialFunction[RawReply, A]): PartialDeserializer[A] =
    new PartialDeserializer[A] {
      def isDefinedAt(x: RawReply) = pf.isDefinedAt(x)
      def apply(x: RawReply) = pf.apply(x)
    }

  def apply[A](prefix: Byte, f: RawReply => A): PartialDeserializer[A] = new PrefixDeserializer[A](prefix, f)

  implicit val intPD      = new PrefixDeserializer[Int]           (Integer, readInt _)
  implicit val longPD      = new PrefixDeserializer[Long]           (Integer, readLong _)
  implicit val stringPD    = new PrefixDeserializer[String]         (Bulk,    readString _)
  implicit val optStringPD = new PrefixDeserializer[Option[String]] (Bulk,    readBulk _)
  implicit val booleanPD = apply(
    new PrefixDeserializer[Boolean](Status, (x: RawReply) => StatusReply(readSingle(x)).asBoolean) orElse
    (longPD andThen (_ > 0)) orElse
    (optStringPD andThen (_.isDefined))
  )

  implicit def multiBulkPD[A](implicit pd: PartialDeserializer[A]) = new PrefixDeserializer[List[A]](Multi, readMultiBulk(_)(pd))

  private[serialization] val errorPD = new PrefixDeserializer[RedisError](Err, readError _)
}

private[serialization] trait LowPriorityPD extends CommandSpecificPD {
  import PartialDeserializer._

  implicit def parsedOptionPD[A](implicit parse: Parse[A]): PartialDeserializer[Option[A]] =
    apply(optStringPD andThen (_ map parse))

  implicit def pairOptionPD[A, B](implicit parseA: Parse[A], parseB: Parse[B]): PartialDeserializer[Option[(A, B)]] =
    apply(listPairPD[A, B] andThen (_.head))

  implicit def listPD[A](implicit parse: Parse[A]): PartialDeserializer[List[A]] =
    apply(multiBulkPD[String] andThen (_.map(parse)))

  implicit def listOptPD[A](implicit parse: Parse[A]): PartialDeserializer[List[Option[A]]] =
    apply(multiBulkPD[Option[String]] andThen (_ map (_ map parse)))

  implicit def setPD[A](implicit parse: Parse[A]): PartialDeserializer[Set[A]] = apply(listPD[A] andThen (_.toSet))

  implicit def listPairPD[A, B](implicit parseA: Parse[A], parseB: Parse[B]): PartialDeserializer[List[Option[(A, B)]]] =
    apply(listOptPD[String] andThen (_.grouped(2).flatMap {
      case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
      case _ => Iterator.single(None)
    }.toList))

  implicit def mapPD[K, V](implicit parseA: Parse[K], parseB: Parse[V]): PartialDeserializer[Map[K, V]] =
    apply(listPairPD[K, V] andThen (_.flatten.toMap))

}

private[serialization] trait CommandSpecificPD { this: LowPriorityPD =>
  import PartialDeserializer._

  // special deserializer for Eval
  implicit def intListPD: PartialDeserializer[List[Int]] = new PrefixDeserializer[List[Int]](Multi, readMultiBulk(_)(intPD))

  // special deserializers for Sorted Set
  import Parse.Implicits._
  implicit def doublePD: PartialDeserializer[Option[Double]] = apply(parsedOptionPD[Double])
  implicit def scoredListPD[A](implicit parseA: Parse[A]): PartialDeserializer[List[(A, Double)]] = apply(
    listPairPD[A, Double] andThen (_.flatten)
  )

}
