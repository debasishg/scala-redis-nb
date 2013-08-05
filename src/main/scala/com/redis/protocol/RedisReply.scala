package com.redis.protocol

import com.redis.serialization.Parse

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
  def asBulk[A: Parse]: Option[A] = ???
  def asBoolean: Boolean = ???
  def asList[A: Parse]: List[Option[A]] = ???
  def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = ???
  def asSet[A: Parse]: Set[A] = ???
}

case class IntegerReply(value: Long) extends RedisReply[Long] {
  final override def asLong = value
  final override def asBoolean: Boolean = value > 0
}

case class StatusReply(value: String) extends RedisReply[String] {
  final override def asString = value
  final override def asBoolean = true
}

case class BulkReply(value: Option[String]) extends RedisReply[Option[String]] {
  final override def asBoolean: Boolean = value.isDefined
  final override def asBulk[A: Parse]: Option[A] = value map implicitly[Parse[A]]
  final override def asString = value.get
}

case class ErrorReply(value: RedisError) extends RedisReply[RedisError] {
  final override def asString = value.message
  final override def asBoolean = false
}

case class MultiBulkReply(value: List[RedisReply[_]]) extends RedisReply[List[RedisReply[_]]] {

  final override def asList[A](implicit parse: Parse[A]) = value.map {
    case BulkReply(strOpt) => strOpt map parse
    case x => Some(parse(x.value.toString))
  }

  final override def asListPairs[A: Parse, B: Parse]: List[Option[(A,B)]] = {
    val parseA = implicitly[Parse[A]]
    val parseB = implicitly[Parse[B]]

    asList[String].grouped(2).flatMap {
      case List(Some(a), Some(b)) => Iterator.single(Some((parseA(a), parseB(b))))
      case _ => Iterator.single(None)
    }.toList
  }

  final override def asSet[A: Parse]: Set[A] = asList[A].flatten.toSet

}