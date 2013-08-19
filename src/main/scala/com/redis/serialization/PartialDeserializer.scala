package com.redis.serialization

import RawReplyReader._
import scala.collection.generic.CanBuildFrom
import scala.collection.{Iterator, GenTraversable}
import scala.language.higherKinds


trait PartialDeserializer[A] extends PartialFunction[RawReply, A] {
  def orElse(pf: PartialFunction[RawReply, A]) = PartialDeserializer(super.orElse(pf))

  override def andThen[B](f: A => B) = PartialDeserializer(super.andThen(f))
}

object PartialDeserializer extends LowPriorityPD {

  def apply[A](pf: PartialFunction[RawReply, A]): PartialDeserializer[A] =
    new PartialDeserializer[A] {
      def isDefinedAt(x: RawReply) = pf.isDefinedAt(x)
      def apply(x: RawReply) = pf.apply(x)
    }

  import PrefixDeserializer._

  implicit val intPD     = _intPD
  implicit val longPD    = _longPD
  implicit val stringPD  = _stringPD
  implicit val bulkPD    = _bulkPD
  implicit val booleanPD = _booleanPD

  implicit def multiBulkPD[A, B[_] <: GenTraversable[_]](implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]) = _multiBulkPD(cbf, pd)
  implicit def listPD[A](implicit pd: PartialDeserializer[A]) = multiBulkPD[A, List]

  val errorPD = _errorPD
}

private[serialization] trait LowPriorityPD extends CommandSpecificPD {
  import PartialDeserializer._

  implicit def parsedPD[A](implicit parse: Read[A]): PartialDeserializer[A] =
    stringPD andThen parse

  implicit def parsedOptionPD[A](implicit parse: Read[A]): PartialDeserializer[Option[A]] =
    bulkPD andThen (_ map parse)

  implicit def setPD[A](implicit parse: Read[A]): PartialDeserializer[Set[A]] =
    multiBulkPD[A, Set]

  implicit def pairOptionListPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[List[Option[(A, B)]]] =
    pairOptionIteratorPD[A, B] andThen (_.toList)

  implicit def pairOptionPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[Option[(A, B)]] =
    pairOptionIteratorPD[A, B] andThen (_.next)

  implicit def mapPD[K, V](implicit parseA: Read[K], parseB: Read[V]): PartialDeserializer[Map[K, V]] =
    pairIteratorPD[K, V] andThen (_.toMap)

  protected def pairIteratorPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[Iterator[(A, B)]] =
    multiBulkPD[String, Iterable] andThen (_.grouped(2).map { case Seq(a, b) => (parseA(a), parseB(b)) })

  protected def pairOptionIteratorPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[Iterator[Option[(A, B)]]] =
    multiBulkPD[Option[String], Iterable] andThen (_.grouped(2).map {
      case Seq(Some(a), Some(b)) => Some((parseA(a), parseB(b)))
      case _ => None
    })
}

private[serialization] trait CommandSpecificPD { this: LowPriorityPD =>
  import PartialDeserializer._

  // special deserializer for Eval
  implicit val intListPD: PartialDeserializer[List[Int]] = multiBulkPD[Int, List]

  // special deserializers for Sorted Set
  import Read.Implicits._
  implicit def doublePD: PartialDeserializer[Option[Double]] = parsedOptionPD[Double]
  implicit def scoredListPD[A](implicit parseA: Read[A]): PartialDeserializer[List[(A, Double)]] =
    pairIteratorPD[A, Double] andThen (_.toList)

  // special deserializer for (H)MGET
  def keyedMapPD[A](fields: Seq[String])(implicit parseV: Read[A]): PartialDeserializer[Map[String, A]] =
    multiBulkPD[Option[A], Iterable] andThen { _.view.zip(fields).collect { case (Some(value), field) => (field, value) }.toMap }
}
