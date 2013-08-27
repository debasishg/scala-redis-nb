package com.redis.serialization

import scala.language.implicitConversions


class Stringified(val value: String) extends AnyVal

object Stringified {
  implicit def apply[A](v: A)(implicit writer: Writer[A]) = new Stringified(writer.toByteString(v).utf8String)

  implicit def applySeq[A](vs: Seq[A])(implicit writer: Writer[A]): Seq[Stringified] = vs.map(apply[A])
}


class KeyValuePair(val pair: Product2[String, Stringified]) extends AnyVal {
  @inline def key   = pair._1
  @inline def value = pair._2
}

object KeyValuePair {

  implicit def apply(pair: Product2[String, Stringified]): KeyValuePair =
    new KeyValuePair(pair)

  implicit def apply[A](pair: Product2[String, A])(implicit writer: Writer[A]): KeyValuePair =
    new KeyValuePair((pair._1, Stringified(pair._2)))

  implicit def applySeq[A](pairs: Seq[Product2[String, A]])(implicit writer: Writer[A]): Seq[KeyValuePair] =
    pairs.map(apply[A])

  implicit def applyIterable[A](pairs: Iterable[Product2[String, A]])(implicit writer: Writer[A]): Iterable[KeyValuePair] =
    pairs.map(apply[A])

  def unapply(kvp: KeyValuePair) = Some(kvp.pair)
}


class ScoredValue(val pair: Product2[Double, Stringified]) extends AnyVal {
  @inline def score = pair._1
  @inline def value = pair._2
}

object ScoredValue {

  implicit def apply(pair: Product2[Double, Stringified]): ScoredValue =
    new ScoredValue(pair)

  implicit def apply[A, B](pair: Product2[A, B])(implicit num: Numeric[A], writer: Writer[B]): ScoredValue =
    new ScoredValue((num.toDouble(pair._1), Stringified(pair._2)))

  implicit def applySeq[A, B](pairs: Seq[Product2[A, B]])(implicit num: Numeric[A], writer: Writer[B]): Seq[ScoredValue] =
    pairs.map(apply[A, B])

  def unapply(sv: ScoredValue) = Some(sv.pair)
}
