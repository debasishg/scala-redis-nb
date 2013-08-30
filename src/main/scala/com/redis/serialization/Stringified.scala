package com.redis.serialization

import akka.util.ByteString
import scala.language.implicitConversions
import scala.collection.generic.CanBuildFrom
import scala.collection.{SeqLike, GenTraversableOnce, GenTraversable}


class Stringified(val value: ByteString) extends AnyVal {
  override def toString = value.utf8String
}


object Stringified {
  implicit def apply[A](v: A)(implicit writer: Writer[A]) = new Stringified(writer.toByteString(v))

  implicit def applySeq[A: Writer](vs: Seq[A]) = vs.map(apply[A])

  implicit class StringifyOps[A: Writer](x: A) {
    def stringify = Stringified(x)
  }
}


class KeyValuePair(val pair: Product2[String, Stringified]) extends AnyVal {
  def key: String       = pair._1
  def value: Stringified = pair._2
}

object KeyValuePair {
  import Stringified._

  implicit def apply(pair: Product2[String, Stringified]): KeyValuePair =
    new KeyValuePair(pair)

  implicit def apply[A: Writer](pair: Product2[String, A]): KeyValuePair =
    new KeyValuePair((pair._1, pair._2.stringify))

  implicit def applySeq[A: Writer](pairs: Seq[Product2[String, A]]): Seq[KeyValuePair] =
    pairs.map(apply[A])

  implicit def applyIterable[A: Writer](pairs: Iterable[Product2[String, A]]): Iterable[KeyValuePair] =
    pairs.map(apply[A])

  def unapply(kvp: KeyValuePair) = Some(kvp.pair)
}


class ScoredValue(val pair: Product2[Double, Stringified]) extends AnyVal {
  def score: Double = pair._1
  def value: Stringified = pair._2
}

object ScoredValue {
  import Stringified._

  implicit def apply(pair: Product2[Double, Stringified]): ScoredValue =
    new ScoredValue(pair)

  implicit def apply[A, B](pair: Product2[A, B])(implicit num: Numeric[A], writer: Writer[B]): ScoredValue =
    new ScoredValue((num.toDouble(pair._1), pair._2.stringify))

  implicit def applySeq[A, B](pairs: Seq[Product2[A, B]])(implicit num: Numeric[A], writer: Writer[B]): Seq[ScoredValue] =
    pairs.map(apply[A, B])

  def unapply(sv: ScoredValue) = Some(sv.pair)
}
