package com.redis.serialization

import RawReplyParser._
import scala.collection.generic.CanBuildFrom
import scala.collection.{Iterator, GenTraversable}
import scala.language.higherKinds
import scala.annotation.implicitNotFound
import com.redis.protocol.Err


@implicitNotFound(msg = "Cannot find implicit PartialDeserializer for ${A}")
trait PartialDeserializer[A] extends PartialFunction[RawReply, A] {
  def orElse(pf: PartialFunction[RawReply, A]) = PartialDeserializer(super.orElse(pf))

  override def andThen[B](f: A => B): PartialDeserializer[B] = PartialDeserializer(super.andThen(f))
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
  implicit val rawBulkPD = _rawBulkPD
  implicit val bulkPD    = _rawBulkPD andThen (_ map (new String(_, "UTF-8")))
  implicit val booleanPD = _booleanPD
  implicit val stringPD  = bulkPD.andThen {
    _.getOrElse { throw new Error("Non-empty bulk reply expected, but got nil") }
  } orElse _statusStringPD

  implicit def multiBulkPD[A, B[_] <: GenTraversable[_]](implicit cbf: CanBuildFrom[_, A, B[A]], pd: PartialDeserializer[A]) = _multiBulkPD(cbf, pd)
  implicit def listPD[A](implicit pd: PartialDeserializer[A]) = multiBulkPD[A, List]

  val errorPD = _errorPD
}

private[serialization] trait LowPriorityPD extends CommandSpecificPD {
  import PartialDeserializer._

  implicit def parsedPD[A](implicit reader: Read[A]): PartialDeserializer[A] =
    stringPD andThen reader.read

  implicit def parsedOptionPD[A](implicit reader: Read[A]): PartialDeserializer[Option[A]] =
    if (reader.forByteArray)
      rawBulkPD.asInstanceOf[PartialDeserializer[Option[A]]]
    else
      bulkPD andThen (_ map reader.read)

  implicit def setPD[A](implicit parse: Read[A]): PartialDeserializer[Set[A]] =
    multiBulkPD[A, Set]

  implicit def pairOptionListPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[List[Option[(A, B)]]] =
    pairOptionIteratorPD[A, B] andThen (_.toList)

  implicit def pairOptionPD[A, B](implicit parseA: Read[A], parseB: Read[B]): PartialDeserializer[Option[(A, B)]] =
    pairOptionIteratorPD[A, B] andThen (_.next)

  implicit def mapPD[K, V](implicit parseA: Read[K], parseB: Read[V]): PartialDeserializer[Map[K, V]] =
    pairIteratorPD[K, V] andThen (_.toMap)

  protected def pairIteratorPD[A, B](implicit readA: Read[A], readB: Read[B]): PartialDeserializer[Iterator[(A, B)]] =
    multiBulkPD[String, Iterable] andThen (_.grouped(2).map { case Seq(a, b) => (readA.read(a), readB.read(b)) })

  protected def pairOptionIteratorPD[A, B](implicit readA: Read[A], readB: Read[B]): PartialDeserializer[Iterator[Option[(A, B)]]] =
    multiBulkPD[Option[String], Iterable] andThen (_.grouped(2).map {
      case Seq(Some(a), Some(b)) => Some((readA.read(a), readB.read(b)))
      case _ => None
    })
}

private[serialization] trait CommandSpecificPD { this: LowPriorityPD =>
  import PartialDeserializer._
  import DefaultFormats._

  // special deserializer for Eval
  implicit val intListPD: PartialDeserializer[List[Int]] = multiBulkPD[Int, List]

  // special deserializers for Sorted Set
  implicit def doubleOptionPD: PartialDeserializer[Option[Double]] = parsedOptionPD[Double]

  implicit def scoredListPD[A](implicit reader: Read[A]): PartialDeserializer[List[(A, Double)]] =
    pairIteratorPD[A, Double] andThen (_.toList)

  // lift non-bulk reply to `Option`
  def liftOptionPD[A](implicit pd: PartialDeserializer[A]): PartialDeserializer[Option[A]] =
    pd.andThen(Option(_)) orElse { case x: RawReply if x.head != Err => None }


  // special deserializer for (H)MGET
  def keyedMapPD[A](fields: Seq[String])(implicit reader: Read[A]): PartialDeserializer[Map[String, A]] =
    multiBulkPD[Option[A], Iterable] andThen { _.view.zip(fields).collect { case (Some(value), field) => (field, value) }.toMap }

  // special deserializer for EVAL(SHA)
  def ensureListPD[A](implicit reader: Read[A]): PartialDeserializer[List[A]] =
    multiBulkPD[A, List].orElse(parsedOptionPD[A].andThen(_.toList))
}
