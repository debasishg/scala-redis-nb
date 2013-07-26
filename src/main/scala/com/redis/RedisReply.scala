package com.redis

import serialization._


/**
 * Redis will reply to commands with different kinds of replies. It is always possible to detect the kind of reply
 * from the first byte sent by the server:
 * <li> In a Status Reply the first byte of the reply is "+"</li>
 * <li> In an Error Reply the first byte of the reply is "-"</li>
 * <li> In an Integer Reply the first byte of the reply is ":"</li>
 * <li> In a Bulk Reply the first byte of the reply is "$"</li>
 * <li> In a Multi Bulk Reply the first byte of the reply s "*"</li>
 */

sealed trait RedisReply[T] {

  def value: T

  def asAny = value.asInstanceOf[Any]

  def asLong: Long =  ???

  def asString: String = ???

  def asBulk[T: Parse]: Option[T] = ???

  def asBoolean: Boolean = ???

  def asList[T: Parse]: List[Option[T]] = ???

  def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = ???

  def asSet[T: Parse]: Set[T] = ???
}

case class IntegerReply(value: Long) extends RedisReply[Long] {
  override def asLong = value
  override def asBoolean: Boolean = value > 0
}

case class StatusReply(value: String) extends RedisReply[String] {
  override def asString = value
  override val asBoolean = true
}

case class BulkReply(value: Option[String]) extends RedisReply[Option[String]] {
  override def asBoolean: Boolean = value.isDefined
  override def asBulk[T: Parse]: Option[T] = value map implicitly[Parse[T]]
  override def asString = value.get
}

case class ErrorReply(value: RedisError) extends RedisReply[RedisError] {
  override def asString = value.message
  override def asBoolean = false
}

case class MultiReply(value: List[RedisReply[_]]) extends RedisReply[List[Any]] {

  override def asList[T: Parse]: List[Option[T]] =
    value.asInstanceOf[List[RedisReply[Option[String]]]].map {
      _.value.map(implicitly[Parse[T]])
    }

  override def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = {
    val parseA = implicitly[Parse[A]]
    val parseB = implicitly[Parse[B]]

    asList[String].grouped(2).flatMap {
      case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
      case _ => Iterator.single(None)
    }.toList
  }

  override def asSet[T: Parse]: Set[T] = asList[T].flatten.toSet

}

