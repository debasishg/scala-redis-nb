package com.redis.serialization

import scala.language.implicitConversions


class Stringified(val string: String) extends AnyVal {
  @inline override def toString = string
}

object Stringified {
  implicit def apply[A](v: A)(implicit writer: Write[A]) = new Stringified(writer.write(v))

  implicit def applySeq[A](vs: Seq[A])(implicit writer: Write[A]): Seq[Stringified] = vs.map(apply[A])
}


class KeyValuePair(val pair: Product2[String, Stringified]) extends AnyVal {
  @inline def key   = pair._1
  @inline def value = pair._2
}

object KeyValuePair {

  implicit def apply[A](pair: Product2[String, A])(implicit writer: Write[A]): KeyValuePair =
    new KeyValuePair((pair._1, Stringified(pair._2)))

  implicit def applySeq[A](pairs: Seq[Product2[String, A]])(implicit writer: Write[A]): Seq[KeyValuePair] =
    pairs.map(apply[A])

  implicit def applyIterable[A](pairs: Iterable[Product2[String, A]])(implicit writer: Write[A]): Iterable[KeyValuePair] =
    pairs.map(apply[A])

  def unapply(kvp: KeyValuePair) = Some(kvp.pair)
}


class ScoredValue(val pair: Product2[Double, Stringified]) extends AnyVal {
  @inline def score = pair._1
  @inline def value = pair._2
}

object ScoredValue {

  implicit def apply[A, B](pair: Product2[A, B])(implicit num: Numeric[A], writer: Write[B]): ScoredValue =
    new ScoredValue((num.toDouble(pair._1), Stringified(pair._2)))

  implicit def applySeq[A, B](pairs: Seq[Product2[A, B]])(implicit num: Numeric[A], writer: Write[B]): Seq[ScoredValue] =
    pairs.map(apply[A, B])

  def unapply(sv: ScoredValue) = Some(sv.pair)
}
